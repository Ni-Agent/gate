package mcptest

import (
	"errors"
	"slices"
)

const GrokClientID = "https://grok.com/oauth/mcp-client.json"
const GrokIdentity = "agent:grok-bot"

// GrokClientMetadata is an operator-reviewed snapshot of Grok's published client
// document. Gate has no arbitrary URL fetching or dynamic client registration.
// Loading the snapshot also works under the service's loopback-only egress policy.
type GrokClientMetadata struct {
	ClientID                string   `json:"client_id"`
	ClientName              string   `json:"client_name"`
	RedirectURIs            []string `json:"redirect_uris"`
	GrantTypes              []string `json:"grant_types"`
	ResponseTypes           []string `json:"response_types"`
	TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method"`
}

func (g *GrokClientMetadata) Validate() error {
	if g.ClientID != GrokClientID || g.ClientName != "Grok" || g.TokenEndpointAuthMethod != "none" || len(g.RedirectURIs) == 0 || len(g.RedirectURIs) > 2 || len(g.GrantTypes) != 2 || !slices.Contains(g.GrantTypes, "authorization_code") || !slices.Contains(g.GrantTypes, "refresh_token") || len(g.ResponseTypes) != 1 || g.ResponseTypes[0] != "code" {
		return errors.New("invalid Grok client metadata; use the reviewed document from " + GrokClientID)
	}
	seen := map[string]bool{}
	for _, redirect := range g.RedirectURIs {
		if seen[redirect] || (redirect != "https://grok.com/connectors-oauth-exchange-code/" && redirect != "https://console.x.ai/connectors-oauth-exchange-code/") {
			return errors.New("untrusted or duplicate Grok OAuth callback")
		}
		seen[redirect] = true
	}
	return nil
}
