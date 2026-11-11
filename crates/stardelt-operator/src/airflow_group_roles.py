# Copy of stardelt-platform/scripts/airflow-group-roles.py (platform#477 S1b), embedded by
# crates/stardelt-operator/src/airflow.rs and mounted at /opt/scripts/sync.py.
#!/usr/bin/env python3
"""Create an Airflow FAB role per ownerGroup on Pipelines and ServicePrincipals."""
import json
import ssl
import urllib.error
import urllib.request

from airflow.providers.fab.auth_manager.cli_commands.utils import get_application_builder


API_HOST = "https://kubernetes.default.svc"
API_BASE = f"{API_HOST}/apis/stardelt.io/v1alpha1"

# The API checks these three resources globally, not per DAG (demos#76): a DAG's access_control
# cannot grant Task Instances / Task Logs, and the DAG read still gates every run, so a global read
# does not leak another group's data.
GROUP_GLOBAL = (
    ("can_read", "DAG Runs"),
    ("can_read", "Task Instances"),
    ("can_read", "Task Logs"),
)


def _sa_token():
    with open("/var/run/secrets/kubernetes.io/serviceaccount/token", "r") as f:
        return f.read().strip()


def _sa_ca():
    return "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


def _namespace():
    with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
        return f.read().strip()


def _api_request(path):
    token = _sa_token()
    ca = _sa_ca()
    req = urllib.request.Request(f"{API_BASE}{path}")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    context = ssl.create_default_context(cafile=ca)
    try:
        with urllib.request.urlopen(req, context=context) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Kubernetes API error {e.code} for {path}: {body}") from e


def main():
    ns = _namespace()
    groups = set()
    for resource in ("pipelines", "serviceprincipals"):
        path = f"/namespaces/{ns}/{resource}"
        data = _api_request(path)
        items = data.get("items") or []
        for item in items:
            spec = item.get("spec") or {}
            owner_group = spec.get("ownerGroup")
            if owner_group:
                owner_group = str(owner_group).strip()
                if owner_group:
                    groups.add(owner_group)

    groups_list = sorted(groups)
    with get_application_builder() as appbuilder:
        sm = getattr(appbuilder, "sm", None)
        for g in groups_list:
            role_name = f"stardelt-group-{g}"
            role = None
            if sm and hasattr(sm, "find_role"):
                try:
                    role = sm.find_role(role_name)
                except Exception:
                    role = None
            if role:
                print(f"exists {role_name}")
            else:
                added = False
                if sm and hasattr(sm, "add_role"):
                    try:
                        sm.add_role(role_name)
                        added = True
                    except Exception:
                        pass
                if not added and sm is not None:
                    # some FAB versions
                    try:
                        sm.add_role(name=role_name)
                        added = True
                    except Exception:
                        pass
                if added:
                    print(f"created {role_name}")
                else:
                    # use session
                    session = appbuilder.session
                    Role = None
                    try:
                        from airflow.providers.fab.auth_manager.models import Role
                    except Exception:
                        try:
                            from airflow.www.fab_security.sqla.models import Role
                        except Exception:
                            Role = None
                    if Role is None:
                        raise RuntimeError(f"no FAB Role model to create {role_name}")
                    role = session.query(Role).filter_by(name=role_name).first()
                    if role:
                        print(f"exists {role_name}")
                    else:
                        role = Role(name=role_name)
                        session.add(role)
                        session.commit()
                        print(f"created {role_name}")
                if role is None and sm and hasattr(sm, "find_role"):
                    role = sm.find_role(role_name)
            if role is None:
                raise RuntimeError(f"could not read back role {role_name}")
            for action, resource in GROUP_GLOBAL:
                sm.add_permission_to_role(role, sm.create_permission(action, resource))
            print(f"converged {role_name}")
    print(f"owner groups: {len(groups_list)}")


if __name__ == "__main__":
    main()
