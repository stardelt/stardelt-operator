# Copy of stardelt-platform/scripts/superset-group-roles.py (platform#494/#508/#513), embedded by
# crates/stardelt-operator/src/superset.rs and mounted at /opt/scripts/sync.py.
# Edit the platform file first, then re-copy.
#!/usr/bin/env python3
"""Create an Superset role per Catalog grant group (platform#494).

Mirrors the Airflow stardelt-group-* model. For each Catalog object (catalogs.stardelt.io)
in the namespace, match it to a Superset Trino database by the URI path (e.g.
`trino://host:443/<catalog>`), and for each grant whose group is not
`stardelt-admins`, converge role `stardelt-group-<group>` to database_access on
that database and datasource_access on its datasets. Drift is removed.
"""

import json
import ssl
import urllib.error
import urllib.request

API_HOST = "https://kubernetes.default.svc"
API_BASE = f"{API_HOST}/apis/stardelt.io/v1alpha1"


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


def catalog_from_uri(uri):
    uri = str(uri or "")
    if "://" not in uri:
        return None
    rest = uri.split("://", 1)[1]
    rest = rest.split("?", 1)[0]
    if "/" not in rest:
        return None
    return rest.split("/", 1)[1].strip() or None


EXTRA = {("can_write", "Dataset"), ("can_duplicate", "Dataset"), ("can_get_or_create_dataset", "Dataset"),
         ("can_save", "Datasource"), ("can_samples", "Datasource"), ("can_get_column_values", "Datasource")}


def is_trino(uri):
    return str(uri or "").lower().startswith("trino://")


def database_name_for_catalog(catalog):
    """`mobility` -> `Trino Mobility`; the platform's existing row names."""
    return "Trino " + str(catalog).replace("-", " ").replace("_", " ").title()


def database_uri_for_catalog(base_uri, catalog):
    """The base Trino URI with its path (the catalog) replaced; credentials/host/port stay."""
    uri = str(base_uri or "")
    if not is_trino(uri):
        raise ValueError(f"not a Trino URI: {uri!r}")
    scheme, rest = uri.split("://", 1)
    rest = rest.split("?", 1)[0]
    if "/" not in rest:
        raise ValueError(f"Trino URI has no catalog path: {uri!r}")
    prefix = rest.split("/", 1)[0]
    return f"{scheme}://{prefix}/{catalog}"


def grants(databases, tables):
    wanted = {("database_access", perm) for perm in databases}
    wanted |= {("datasource_access", perm) for perm in tables if perm}
    return wanted


def converge(current, want, granted):
    keep = want | granted
    return sorted(keep - current), sorted(current - keep)


def database_perms(granted):
    """The database perm strings in a `grants(...)` set (datasource perms dropped).

    `grants` returns `(permission, view-menu)` tuples; the dashboard side holds
    `Database.perm` strings. Mixing the two makes every subset check false and every
    dashboard admin-only, which is how the first live run behaved.
    """
    return {perm for permission, perm in granted if permission == "database_access"}


def dashboard_roles(chart_databases, role_databases):
    """The group roles that may see a dashboard: every chart's database must be covered.

    `chart_databases` is the set of database perms the dashboard's charts read;
    `role_databases` maps a role name to the database perms it holds. An empty chart set
    (no chart, or a chart without a resolvable Trino datasource) has no group owner and
    stays admin-only: [].
    """
    if not chart_databases:
        return []
    return sorted(role for role, dbs in role_databases.items() if chart_databases <= dbs)


def main():
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset import db, security_manager as sm
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database

        ns = _namespace()
        try:
            data = _api_request(f"/namespaces/{ns}/catalogs")
        except Exception as e:
            print(f"failed to list catalogs: {e}")
            raise
        catalogs = data.get("items") or []

        # Build map from catalog name -> (db, datasets)
        trino_dbs = [d for d in db.session.query(Database).order_by(Database.id) if is_trino(d.sqlalchemy_uri)]
        db_by_catalog = {}
        for d in trino_dbs:
            cat = catalog_from_uri(d.sqlalchemy_uri)
            if cat:
                db_by_catalog[cat] = d

        names_fn = lambda r: {(p.permission.name, p.view_menu.name) for p in r.permissions}

        role_databases = {}
        for cat in catalogs:
            meta = cat.get("metadata") or {}
            spec = cat.get("spec") or {}
            cat_name = meta.get("name")
            if not cat_name:
                continue
            db_obj = db_by_catalog.get(cat_name)
            if not db_obj:
                if not trino_dbs:
                    raise SystemExit("no Trino database in Superset to copy the connection from")
                base = trino_dbs[0]
                db_obj = Database(
                    database_name=database_name_for_catalog(cat_name),
                    sqlalchemy_uri=database_uri_for_catalog(base.sqlalchemy_uri, cat_name),
                    impersonate_user=bool(base.impersonate_user),
                    extra=base.extra,
                )
                db.session.add(db_obj)
                db.session.commit()
                db_by_catalog[cat_name] = db_obj
                print(f"created {db_obj.database_name} for catalog {cat_name}")
            grants_list = spec.get("grants") or []
            groups = set()
            for g in grants_list:
                if not isinstance(g, dict):
                    continue
                grp = g.get("group")
                if grp and str(grp).strip() and str(grp).strip() != "stardelt-admins":
                    groups.add(str(grp).strip())
            if not groups:
                continue
            # Get datasets for this database
            tables = db.session.query(SqlaTable).filter(SqlaTable.database_id == db_obj.id).all()
            granted_all = grants([db_obj.perm], [t.perm for t in tables])
            granted = database_perms(granted_all)
            want = set()  # per-group roles only get what's granted for this catalog; they also get Gamma/stardelt-analyst via mapping? The ticket says "stardelt-analyst keeps Gamma + SQL Lab but loses the blanket..." and group roles carry catalogs. Also in mapping we add the group role to users in that group - so the role itself needs the permissions.
            for grp in sorted(groups):
                role_name = f"stardelt-group-{grp}"
                role = sm.find_role(role_name) or sm.add_role(role_name)
                add, remove = converge(names_fn(role), want, granted_all)
                for n, v in add:
                    pv = sm.find_permission_view_menu(n, v)
                    if pv:
                        sm.add_permission_role(role, pv)
                for n, v in remove:
                    pv = sm.find_permission_view_menu(n, v)
                    if pv:
                        sm.del_permission_role(role, pv)
                role_databases.setdefault(role_name, set()).update(granted)
                print(f"role {role_name}: {len(role.permissions)} permissions (db {db_obj.database_name})")
        from superset.models.dashboard import Dashboard

        for dashboard in db.session.query(Dashboard).filter(Dashboard.published.is_(True)).all():
            chart_dbs = set()
            unresolved = False
            for chart in dashboard.slices:
                ds = chart.datasource
                if ds is None or getattr(ds, "database", None) is None:
                    unresolved = True
                    continue
                if not is_trino(ds.database.sqlalchemy_uri):
                    unresolved = True
                    continue
                chart_dbs.add(ds.database.perm)
            names = [] if unresolved else dashboard_roles(chart_dbs, role_databases)
            wanted = [sm.find_role(name) for name in names]
            if [r.name for r in dashboard.roles] != names:
                dashboard.roles = [r for r in wanted if r is not None]
                db.session.commit()
                print(f"dashboard {dashboard.dashboard_title}: {names or 'admin-only'}")
            else:
                print(f"dashboard {dashboard.dashboard_title}: {names or 'admin-only'}")
        db.session.commit()
        print("superset-group-roles: done")


if __name__ == "__main__":
    main()
