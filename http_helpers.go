package ratelimiter

import (
	"net"
	"net/http"
)

func HeaderKeyFunc(header string) func(*http.Request) Key {
	return func(r *http.Request) Key {
		if r == nil {
			return ""
		}
		return Key(r.Header.Get(header))
	}
}

func RemoteIPKeyFunc() func(*http.Request) Key {
	return func(r *http.Request) Key {
		if r == nil {
			return ""
		}

		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err == nil {
			return Key(host)
		}

		return Key(r.RemoteAddr)
	}
}