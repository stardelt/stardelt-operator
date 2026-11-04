# Copy of stardelt-platform/helm-values/superset.yaml `configOverrides.oauth`
# (stardelt-platform#501/#505/#510): sign-in through the platform IdP, the Keycloak
# subject as the Superset/Trino principal, the role mapping (stardelt-admins -> Admin,
# stardelt-users -> Gamma + stardelt-analyst, stardelt-group-<g> lazily) and the Trino
# service-account password store. Injected into the golden values by
# `crates/stardelt-operator/src/values.rs`; edit the platform file first, then re-copy.
import os
from flask_appbuilder.const import AUTH_DB, AUTH_OAUTH
from superset.security import SupersetSecurityManager

# From the stardelt-identity ConfigMap and stardelt-oidc-clients Secret,
# injected as env by extraEnvRaw above. Never literals in this file.
_issuer = os.environ.get("STARDELT_OIDC_ISSUER", "").rstrip("/")
_secret = os.environ.get("STARDELT_SUPERSET_CLIENT_SECRET", "")
_local_only = os.environ.get("STARDELT_AUTH_LOCAL_ONLY") == "1"

if _issuer and _secret and not _local_only:
    AUTH_TYPE = AUTH_OAUTH
    OAUTH_PROVIDERS = [
        {
            "name": "keycloak",
            "token_key": "access_token",
            "icon": "fa-key",
            "remote_app": {
                "client_id": "superset",
                "client_secret": _secret,
                # Discovery carries the token/auth endpoints; api_base_url is
                # what FAB's Keycloak branch joins "openid-connect/userinfo"
                # onto to read groups.
                "server_metadata_url": _issuer + "/.well-known/openid-configuration",
                "api_base_url": _issuer + "/protocol/",
                "client_kwargs": {"scope": "openid email profile"},
            },
        }
    ]
    # Anyone in the realm may sign in (Gamma: read shared dashboards);
    # members of stardelt-admins are synced to Admin on every login, and
    # members of stardelt-users to Gamma + stardelt-analyst (SQL Lab and the
    # datasets of the `shared` catalog, nothing administrative; the roles are
    # created by the init Jobs — #287, #494). Synced on every login, so leaving
    # the group takes the role away at the next sign-in. Per-group roles are
    # mapped lazily from existing `stardelt-group-<g>` roles.
    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = "Gamma"

    class _AuthRolesMapping(dict):
        def items(self):
            try:
                from superset import security_manager as sm

                base = {}
                try:
                    base.update(dict(super().items()))
                except Exception:
                    pass
                mapping = {}
                mapping[os.environ.get("STARDELT_ADMIN_GROUP", "stardelt-admins")] = base.get(
                    os.environ.get("STARDELT_ADMIN_GROUP", "stardelt-admins"), ["Admin"]
                )
                mapping["stardelt-users"] = base.get("stardelt-users", ["Gamma", "stardelt-analyst"])
                try:
                    roles = sm.get_all_roles() or []
                except Exception:
                    roles = []
                for role in roles:
                    name = getattr(role, "name", None)
                    if not name or not name.startswith("stardelt-group-"):
                        continue
                    group = name[len("stardelt-group-"):]
                    if not group:
                        continue
                    mapping[group] = ["Gamma", "stardelt-analyst", name]
                return mapping.items()
            except Exception:
                try:
                    return dict(super().items()).items()
                except Exception:
                    return [
                        (os.environ.get("STARDELT_ADMIN_GROUP", "stardelt-admins"), ["Admin"]),
                        ("stardelt-users", ["Gamma", "stardelt-analyst"]),
                    ]

    AUTH_ROLES_MAPPING = _AuthRolesMapping(
        {
            os.environ.get("STARDELT_ADMIN_GROUP", "stardelt-admins"): ["Admin"],
            "stardelt-users": ["Gamma", "stardelt-analyst"],
        }
    )
    AUTH_ROLES_SYNC_AT_LOGIN = True

    # --- the Superset username is the Keycloak subject (#310) -------------
    # The person who signs in here is the person Trino authorizes: the form
    # login name is not what the catalog knows. After the identity turn-on
    # (authz 8/8) Trino's principal is the token's `sub` and the permission
    # bridge maps that principal to the Lakekeeper user `oidc~<sub>`, so the
    # name Superset impersonates must be that same UUID. Readable names do not
    # disappear: first and last name come from the same userinfo response and
    # Superset shows those.
    #
    # FAB's Keycloak branch (flask_appbuilder/security/manager.py,
    # `get_oauth_user_info`) returns `preferred_username`; this subclass keeps
    # that branch's keys and swaps only the username. `oauth_user_info` is the
    # hook FAB's OAuth view calls (it binds the instance attribute in __init__
    # unless a subclass already provides one), and the fallback names
    # `get_oauth_user_info` because `BaseSecurityManager.oauth_user_info`
    # itself is None in FAB 4.5.5 — checked in the running pod, not guessed.
    class StardeltSecurityManager(SupersetSecurityManager):
        """Sign people in under their Keycloak subject (`sub`), not their login name."""

        @staticmethod
        def _rename_alias_row(subject, email):
            """Adopt the row a person already has from before #310.

            People who signed in before this change have a row under their login
            name, and FAB's `ab_user.email` is UNIQUE *and* NOT NULL — a second row
            for the same address cannot be inserted. Measured on this cluster: the
            next sign-in ended 401 with `duplicate key value violates unique
            constraint "ab_user_email_key"` when the old row already held the
            address (the ticket expected a new row; the column forbids it). Rename
            that row instead: one row per person, the same email and saved queries,
            and the username (the name Trino sees) becomes the `sub`.
            """
            if not email:
                return
            from flask_appbuilder.security.sqla.models import User
            from superset import db

            alias = db.session.query(User).filter(User.email == email).one_or_none()
            if alias is None or alias.username == subject:
                return
            if db.session.query(User).filter(User.username == subject).one_or_none():
                return  # somebody already signs in under the subject: merge nothing
            alias.username = subject
            db.session.commit()

        def oauth_user_info(self, provider, response=None):
            if provider == "keycloak":
                me = self.appbuilder.sm.oauth_remotes[provider].get(
                    "openid-connect/userinfo"
                )
                me.raise_for_status()
                data = me.json()
                self._rename_alias_row(data["sub"], data.get("email", ""))
                return {
                    "username": data["sub"],
                    "first_name": data.get("given_name", ""),
                    "last_name": data.get("family_name", ""),
                    "email": data.get("email", ""),
                    "role_keys": data.get("groups", []),
                }
            return super().get_oauth_user_info(provider, response)

    CUSTOM_SECURITY_MANAGER = StardeltSecurityManager
else:
    AUTH_TYPE = AUTH_DB

# --- one result cache per person (#310) -----------------------------------
# With impersonation on, a chart's query runs as the viewer; without this flag
# Superset keys the result cache by the query alone, and user B is served user
# A's cached chart (measured in platform#279, Q6 — a cross-user data leak).
# `globals()` is empty here: Superset loads this file as its own module and
# copies the upper-case names onto superset.config, whose FEATURE_FLAGS is {}.
# Outside the branch on purpose: switching back to the local form must not
# silently drop the flag.
FEATURE_FLAGS = {**globals().get("FEATURE_FLAGS", {}), "CACHE_IMPERSONATION": True}

# --- Trino service account (platform#278) ---------------------------------
# The datasource URI carries no password (extraConfigs/import_datasources.yaml):
# Superset asks this hook for it every time it builds an engine, so the
# credential lives in exactly one place — Secret trino-password-db, via the pod
# environment. Rotating the Secret and restarting the pods is the whole story.
# The hook is global (every database goes through it), so it answers only for
# our Trino service account and hands every other database its URL password.
def _stardelt_password_store(url):
    """The Trino service account's password comes from the environment, never from the stored URI.
    The hook is global (every database goes through it), so it answers only for our Trino account."""
    if url.drivername.startswith("trino") and url.username == "superset":
        # No service account on an install without `make install-identity`: the
        # variable is absent, Trino has no authentication, and the
        # credential-free URI stays exactly as it was.
        return os.environ.get("TRINO_SUPERSET_PASSWORD") or url.password
    return url.password

SQLALCHEMY_CUSTOM_PASSWORD_STORE = _stardelt_password_store
