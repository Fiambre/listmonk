package email

import (
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strings"
	"sync"
	"time"

	"github.com/knadh/listmonk/internal/utils"
	"github.com/knadh/listmonk/models"
	"github.com/knadh/smtppool/v2"
)

const (
	MessengerName = "email"

	hdrReturnPath = "Return-Path"
	hdrBcc        = "Bcc"
	hdrCc         = "Cc"
	hdrMessageID  = "Message-Id"
)

// ErrQuotaExhausted is returned by Push() when all SMTP servers in the pool
// have reached their daily send quota.
var ErrQuotaExhausted = errors.New("all SMTP servers have reached their daily send quota")

// quotaState tracks daily send count per server. Reset at midnight UTC.
type quotaState struct {
	mu        sync.Mutex
	sentToday int64
}

// rateState tracks per-second and sliding-window rate limiting per server.
type rateState struct {
	mu          sync.Mutex
	lastSent    time.Time
	minInterval time.Duration // time.Second / MaxRate

	windowStart time.Time
	windowCount int
	windowDur   time.Duration
}

// Server represents an SMTP server's credentials.
type Server struct {
	// Name is a unique identifier for the server.
	Name          string            `json:"name"`
	Username      string            `json:"username"`
	Password      string            `json:"password"`
	AuthProtocol  string            `json:"auth_protocol"`
	TLSType       string            `json:"tls_type"`
	TLSSkipVerify bool              `json:"tls_skip_verify"`
	EmailHeaders  map[string]string `json:"email_headers"`
	FromAddresses []string          `json:"from_addresses"`

	MaxRate               int    `json:"max_rate"`
	SlidingWindow         bool   `json:"sliding_window"`
	SlidingWindowRate     int    `json:"sliding_window_rate"`
	SlidingWindowDuration string `json:"sliding_window_duration"`
	DailySendQuota        int    `json:"daily_send_quota"`

	// Rest of the options are embedded directly from the smtppool lib.
	// The JSON tag is for config unmarshal to work.
	//lint:ignore SA5008 ,squash is needed by koanf/mapstructure config unmarshal.
	smtppool.Opt `json:",squash"`

	pool  *smtppool.Pool
	quota *quotaState
	rate  *rateState
}

// Emailer is the SMTP e-mail messenger.
type Emailer struct {
	name string

	// pools holds groups of SMTP servers indexed by a key ('from'-address
	// or a domain set per SMTPs server). An empty key holds all servers
	// and is the fallback round-robin when there's no match (old behaviour).
	pools map[string][]*Server

	// onQuotaReset is called after daily quota counters are reset at midnight UTC.
	// Used to resume campaigns paused due to quota exhaustion.
	onQuotaReset func()
}

// NormalizeAddr normalizes an e-mail address (strip spaces, lowercase).
func NormalizeAddr(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// New returns an SMTP e-mail Messenger backend with the given SMTP servers.
// onQuotaReset is called at midnight UTC after daily counters reset; pass nil to disable.
func New(name string, onQuotaReset func(), servers ...Server) (*Emailer, error) {
	e := &Emailer{
		name:         name,
		pools:        make(map[string][]*Server),
		onQuotaReset: onQuotaReset,
	}

	for _, srv := range servers {
		s := srv

		var auth smtp.Auth
		switch s.AuthProtocol {
		case "cram":
			auth = smtp.CRAMMD5Auth(s.Username, s.Password)
		case "plain":
			auth = smtp.PlainAuth("", s.Username, s.Password, s.Host)
		case "login":
			auth = &smtppool.LoginAuth{Username: s.Username, Password: s.Password}
		case "", "none":
		default:
			return nil, fmt.Errorf("unknown SMTP auth type '%s'", s.AuthProtocol)
		}
		s.Opt.Auth = auth

		// TLS config.
		s.Opt.SSL = smtppool.SSLNone
		if s.TLSType != "none" {
			s.TLSConfig = &tls.Config{}
			if s.TLSSkipVerify {
				s.TLSConfig.InsecureSkipVerify = s.TLSSkipVerify
			} else {
				s.TLSConfig.ServerName = s.Host
			}

			// SSL/TLS, not STARTTLS.
			switch s.TLSType {
			case "TLS":
				s.Opt.SSL = smtppool.SSLTLS
			case "STARTTLS":
				s.Opt.SSL = smtppool.SSLSTARTTLS
			}
		}

		pool, err := smtppool.New(s.Opt)
		if err != nil {
			return nil, err
		}

		s.pool = pool
		s.quota = &quotaState{}
		s.rate = &rateState{
			windowStart: time.Now(),
		}
		if s.MaxRate > 0 {
			s.rate.minInterval = time.Second / time.Duration(s.MaxRate)
		}
		if s.SlidingWindowDuration != "" {
			d, _ := time.ParseDuration(s.SlidingWindowDuration)
			s.rate.windowDur = d
		}

		// Add to the global list (empty key) and to each from-address
		// bucket. Duplicate keys across servers are fine and get round-robin'd.
		e.pools[""] = append(e.pools[""], &s)
		for _, addr := range s.FromAddresses {
			if key := NormalizeAddr(addr); key != "" {
				e.pools[key] = append(e.pools[key], &s)
			}
		}
	}

	go e.runQuotaReset()

	return e, nil
}

// Name returns the messenger's name.
func (e *Emailer) Name() string {
	return e.name
}

// Push pushes a message to the server.
func (e *Emailer) Push(m models.Message) error {
	// Pick the from-address-routed pool if there is one, else default
	// to the full pool (empty key) for roundrobin.
	pool := e.pools[""]
	if len(e.pools) > 1 {
		if srvs := e.getPool(m.From); srvs != nil {
			pool = srvs
		}
	}

	// Filter out quota-exhausted servers.
	eligible := make([]*Server, 0, len(pool))
	for _, s := range pool {
		if !s.isQuotaExhausted() {
			eligible = append(eligible, s)
		}
	}
	if len(eligible) == 0 {
		return ErrQuotaExhausted
	}

	srv := eligible[rand.Intn(len(eligible))]

	// Apply per-SMTP rate limiting (may sleep briefly).
	srv.applyRateLimit()

	// Are there attachments?
	var files []smtppool.Attachment
	if m.Attachments != nil {
		files = make([]smtppool.Attachment, 0, len(m.Attachments))
		for _, f := range m.Attachments {
			a := smtppool.Attachment{
				Filename: f.Name,
				Header:   f.Header,
				Content:  make([]byte, len(f.Content)),
			}
			copy(a.Content, f.Content)
			files = append(files, a)
		}
	}

	// Create the email.
	em := smtppool.Email{
		From:        m.From,
		To:          m.To,
		Subject:     m.Subject,
		Attachments: files,
	}

	em.Headers = textproto.MIMEHeader{}

	// Attach SMTP level headers.
	for k, v := range srv.EmailHeaders {
		em.Headers.Set(k, v)
	}

	// Attach e-mail level headers.
	for k, v := range m.Headers {
		em.Headers.Set(k, v[0])
	}

	// Generate Message-Id based on the From address.
	if em.Headers.Get(hdrMessageID) == "" {
		d := "localhost"
		if a, err := mail.ParseAddress(m.From); err == nil {
			d = a.Address[strings.LastIndex(a.Address, "@")+1:]
		}
		if r, err := utils.GenerateRandomString(24); err == nil {
			em.Headers.Set(hdrMessageID, fmt.Sprintf("<%s@%s>", r, d))
		}
	}

	// If the `Return-Path` header is set, it should be set as the
	// the SMTP envelope sender (via the Sender field of the email struct).
	if sender := em.Headers.Get(hdrReturnPath); sender != "" {
		em.Sender = sender
		em.Headers.Del(hdrReturnPath)
	}

	// If the `Bcc` header is set, it should be set on the Envelope
	if bcc := em.Headers.Get(hdrBcc); bcc != "" {
		for _, part := range strings.Split(bcc, ",") {
			em.Bcc = append(em.Bcc, strings.TrimSpace(part))
		}
		em.Headers.Del(hdrBcc)
	}

	// If the `Cc` header is set, it should be set on the Envelope
	if cc := em.Headers.Get(hdrCc); cc != "" {
		for _, part := range strings.Split(cc, ",") {
			em.Cc = append(em.Cc, strings.TrimSpace(part))
		}
		em.Headers.Del(hdrCc)
	}

	switch m.ContentType {
	case "plain":
		em.Text = []byte(m.Body)
	default:
		em.HTML = m.Body
		if len(m.AltBody) > 0 {
			em.Text = m.AltBody
		}
	}

	if err := srv.pool.Send(em); err != nil {
		return err
	}

	srv.incrementQuota()
	return nil
}

// Flush flushes the message queue to the server.
func (e *Emailer) Flush() error {
	return nil
}

// Close closes the SMTP pools.
func (e *Emailer) Close() error {
	for _, s := range e.pools[""] {
		s.pool.Close()
	}
	return nil
}

// runQuotaReset resets all server daily counters at UTC midnight and calls onQuotaReset.
// Runs for the lifetime of the Emailer.
func (e *Emailer) runQuotaReset() {
	for {
		time.Sleep(time.Until(nextMidnightUTC()))
		for _, srv := range e.pools[""] {
			srv.quota.mu.Lock()
			srv.quota.sentToday = 0
			srv.quota.mu.Unlock()
		}
		if e.onQuotaReset != nil {
			e.onQuotaReset()
		}
	}
}

// nextMidnightUTC returns the next UTC midnight.
func nextMidnightUTC() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
}

// isQuotaExhausted reports whether this server has reached its daily send limit.
func (s *Server) isQuotaExhausted() bool {
	if s.DailySendQuota <= 0 {
		return false
	}
	s.quota.mu.Lock()
	defer s.quota.mu.Unlock()
	return s.quota.sentToday >= int64(s.DailySendQuota)
}

// incrementQuota increments the daily sent counter after a successful send.
func (s *Server) incrementQuota() {
	if s.DailySendQuota <= 0 {
		return
	}
	s.quota.mu.Lock()
	s.quota.sentToday++
	s.quota.mu.Unlock()
}

// applyRateLimit sleeps if needed to respect per-second and sliding-window limits.
// Each concurrent caller "reserves" its send slot so limits are correctly shared.
func (s *Server) applyRateLimit() {
	// Per-second rate limit.
	if s.MaxRate > 0 && s.rate.minInterval > 0 {
		s.rate.mu.Lock()
		now := time.Now()
		var wait time.Duration
		if elapsed := now.Sub(s.rate.lastSent); elapsed < s.rate.minInterval {
			wait = s.rate.minInterval - elapsed
		}
		s.rate.lastSent = now.Add(wait)
		s.rate.mu.Unlock()
		if wait > 0 {
			time.Sleep(wait)
		}
	}

	// Sliding window rate limit.
	if s.SlidingWindow && s.SlidingWindowRate > 0 && s.rate.windowDur.Seconds() > 1 {
		s.rate.mu.Lock()
		now := time.Now()
		diff := now.Sub(s.rate.windowStart)
		if diff >= s.rate.windowDur {
			s.rate.windowStart = now
			s.rate.windowCount = 0
		}
		s.rate.windowCount++
		var wait time.Duration
		if s.rate.windowCount >= s.SlidingWindowRate {
			wait = s.rate.windowDur - diff
			s.rate.windowStart = now.Add(wait)
			s.rate.windowCount = 0
		}
		s.rate.mu.Unlock()
		if wait > 0 {
			time.Sleep(wait)
		}
	}
}

// getPool returns the pool of servers configured to handle the given From
// header, matched by full e-mail and then by domain.
// Returns nil if no mapping matches.
func (e *Emailer) getPool(from string) []*Server {
	addr := utils.ParseEmailAddress(from)
	if addr == "" {
		return nil
	}

	if srvs, ok := e.pools[addr]; ok {
		return srvs
	}

	if _, after, ok := strings.Cut(addr, "@"); ok {
		return e.pools[after]
	}

	return nil
}
