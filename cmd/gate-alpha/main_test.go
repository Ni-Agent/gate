package main

import "testing"

func TestReviewHTTPBindingIsExplicitLoopbackOnly(t *testing.T) {
	for _, a := range []string{"127.0.0.1:8787", "[::1]:8787"} {
		if e := validLoopback(a); e != nil {
			t.Fatal(a, e)
		}
	}
	for _, a := range []string{"0.0.0.0:8787", "[::]:8787", "192.0.2.111:8787", "localhost:8787", "127.0.0.1:0", "127.0.0.1:08787", "127.0.0.1:65536", "127.0.0.1"} {
		if e := validLoopback(a); e == nil {
			t.Fatal("unsafe or malformed binding accepted", a)
		}
	}
}
