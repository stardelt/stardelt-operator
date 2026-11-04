import os

# Keycloak's endpoints are derived from the identity contract's issuer URL
# (https://id.<baseDomain>/realms/stardelt), never written into this file.
_issuer = os.environ.get("STARDELT_OIDC_ISSUER_URL", "").rstrip("/")
if _issuer:
    c.GenericOAuthenticator.authorize_url = _issuer + "/protocol/openid-connect/auth"
    c.GenericOAuthenticator.token_url = _issuer + "/protocol/openid-connect/token"
    c.GenericOAuthenticator.userdata_url = _issuer + "/protocol/openid-connect/userinfo"

# The callback Keycloak must be told is the *public* one. Left to itself the
# hub advertises http://proxy-public/hub/oauth_callback — its internal service
# name, which Keycloak does not have registered, so the login dies as
# invalid_redirect_uri before a code is ever issued. The hub host follows the
# platform convention jupyter.<base>, and the base is the issuer's host minus
# the leading "id." label.
_host = _issuer.split("://", 1)[-1].split("/", 1)[0] if _issuer else ""
_base = _host.split(".", 1)[1] if "." in _host else ""
_callback = os.environ.get("STARDELT_HUB_OAUTH_CALLBACK_URL", "")
if not _callback and _base:
    _callback = "https://jupyter." + _base + "/hub/oauth_callback"
if _callback:
    c.GenericOAuthenticator.oauth_callback_url = _callback

_secret = os.environ.get("STARDELT_OIDC_CLIENT_SECRET", "").strip()
if _secret:
    c.GenericOAuthenticator.client_secret = _secret


# The signed-in user's own token inside the spawned notebook (the platform's
# 20-notebook-identity): JupyterHub keeps the OAuth token only with auth_state on.
if _issuer:
    def _trino_token(spawner, auth_state):
        token = (auth_state or {}).get("access_token")
        if token:
            spawner.environment["TRINO_TOKEN"] = token

    c.Authenticator.enable_auth_state = True
    c.Spawner.auth_state_hook = _trino_token
    c.OAuthenticator.refresh_pre_spawn = True
    c.Authenticator.delete_invalid_users = True

    # The warehouse endpoint: Trino serves TLS on the in-cluster Service and refuses a
    # plain-HTTP statement, so the pod gets that endpoint and the CA that signs it —
    # never the public host and never a disabled certificate check.
    c.KubeSpawner.environment = {
        **(c.KubeSpawner.environment or {}),
        "TRINO_URL": os.environ.get(
            "STARDELT_TRINO_URL", "https://trino.stardelt.svc.cluster.local:8443"
        ),
        "REQUESTS_CA_BUNDLE": "/etc/stardelt/trino-ca/ca.crt",
    }
