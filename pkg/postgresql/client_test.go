package postgresql

import (
	"context"
	"strings"
	"testing"
)

// connect must never put the password from DATABASE_URL into its error.
func TestConnectErrorHidesPassword(t *testing.T) {
	for _, url := range []string{
		"postgres://adapter:s3cret@127.0.0.1:notaport/db",                                              // parse error
		"postgres://adapter:s3cret@127.0.0.1:1/db?connect_timeout=2",                                   // dial error
		"postgres://adapter:s3cret@127.0.0.1:1,127.0.0.1:2/db?connect_timeout=2",                       // multi-host, as on pskep
		"postgres://adapter:s3cret@127.0.0.1:1/db?sslmode=verify-full&sslrootcert=/nonexistent/ca.crt", // TLS config error
	} {
		t.Setenv("DATABASE_URL", url)
		_, err := connect(context.Background())
		if err == nil {
			t.Fatalf("%s: expected an error", url)
		}
		if strings.Contains(err.Error(), "s3cret") {
			t.Errorf("password leaked: %v", err)
		}
	}
}
