package auth

import (
	"context"
	"testing"
)

func TestAccessListAllows(t *testing.T) {
	al := AccessList{VerticalIDs: []string{"bus"}, TenantIDs: []string{"launch-ac"}}
	cases := []struct {
		v, t string
		want bool
	}{
		{"bus", "", true},
		{"bus", "bus-ac", true}, // vertical grant covers its tenants
		{"launch", "launch-ac", true},
		{"launch", "launch-nonac", false},
		{"", "", false},
	}
	for _, c := range cases {
		if got := al.Allows(c.v, c.t); got != c.want {
			t.Errorf("Allows(%q,%q)=%v want %v", c.v, c.t, got, c.want)
		}
	}
	if !(AccessList{WholeCompany: true}).Allows("x", "y") {
		t.Error("whole company must allow everything")
	}
}

func TestDisablingAuthRefusedOutsideLocal(t *testing.T) {
	for _, env := range []string{"staging", "production"} {
		if _, err := NewVerifier(context.Background(), Config{Enabled: false}, env); err == nil {
			t.Errorf("AUTH_ENABLED=false must be refused in %s", env)
		}
	}
	if _, err := NewVerifier(context.Background(), Config{Enabled: false}, "local"); err != nil {
		t.Errorf("local: %v", err)
	}
	if _, err := NewAuthorizer("allow-all", "production"); err == nil {
		t.Error("allow-all must be refused in production")
	}
}

func TestBearer(t *testing.T) {
	if bearer("Bearer abc") != "abc" || bearer("bearer  abc ") != "abc" || bearer("Basic x") != "" {
		t.Fatal("bearer parsing")
	}
}

func TestPrincipalActor(t *testing.T) {
	svc := Principal{Subject: "booking", ClientID: "booking"}
	usr := Principal{Subject: "u1", ClientID: "portal"}
	if !svc.IsService() || svc.Actor().Type != "service" || usr.IsService() || usr.Actor().ID != "u1" {
		t.Fatal("actor mapping")
	}
}
