# Copy of stardelt-platform/scripts/superset-analyst-role.py (stardelt-platform#287/#494),
# injected into the Superset init Job's extraConfigs by crates/stardelt-operator/src/values.rs.
# Edit the platform file first, then re-copy.
#!/usr/bin/env python3
"""Create/refresh the Superset role for normal platform users (platform#287, platform#482).

`stardelt-analyst` is the built-in `Gamma` and `sql_lab` permission sets plus
Alpha's dataset/datasource additions and `database_access` on the `shared`
Trino database only (`datasource_access` on its datasets): SQL Lab and the
shared area, nothing administrative (no `can_write` on Database, no
`all_database_access` / `all_datasource_access`, no `can_upload` on Database). A
database counts as Trino when its URI starts with `trino://`; Trino stays the
authority for the data. The init Job runs this after `superset init` and after
the datasource import, so a fresh install has the role before the first login;
`AUTH_ROLES_MAPPING` then syncs it from group `stardelt-users`.

Idempotent: it converges the role to exactly that permission set (drift is
removed) and prints one line. Run it inside the Superset app environment:

    kubectl -n stardelt exec deploy/superset -- python /app/configs/superset-analyst-role.py
"""

EXTRA = {("can_write", "Dataset"), ("can_duplicate", "Dataset"), ("can_get_or_create_dataset", "Dataset"),
         ("can_save", "Datasource"), ("can_samples", "Datasource"), ("can_get_column_values", "Datasource")}


def is_trino(uri):
    """Whether a Superset database URI is one of the platform's Trino databases."""
    return str(uri or "").lower().startswith("trino://")


def catalog_from_uri(uri):
    uri = str(uri or "")
    if "://" not in uri:
        return None
    rest = uri.split("://", 1)[1]
    rest = rest.split("?", 1)[0]
    if "/" not in rest:
        return None
    return rest.split("/", 1)[1].strip() or None


def grants(databases, tables):
    """The (permission, view menu) pairs for `database_access` on each database and `datasource_access` on each dataset."""
    wanted = {("database_access", perm) for perm in databases}
    wanted |= {("datasource_access", perm) for perm in tables if perm}
    return wanted


def converge(current, want, granted):
    """What to add to and what to remove from a role that holds `current`: exactly `want` plus `granted`."""
    keep = want | granted
    return sorted(keep - current), sorted(current - keep)


def main():
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset import db, security_manager as sm
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database

        trino = [d for d in db.session.query(Database).order_by(Database.id) if is_trino(d.sqlalchemy_uri) and catalog_from_uri(d.sqlalchemy_uri) == "shared"]
        if not trino:
            raise SystemExit("no Trino database for catalog shared in Superset: the role would give access to nothing")
        tables = db.session.query(SqlaTable).filter(SqlaTable.database_id.in_([d.id for d in trino])).all()
        names = lambda r: {(p.permission.name, p.view_menu.name) for p in r.permissions}
        gamma, alpha, sqll = (sm.find_role(n) for n in ("Gamma", "Alpha", "sql_lab"))
        want = names(gamma) | names(sqll) | (names(alpha) & EXTRA)
        role = sm.find_role("stardelt-analyst") or sm.add_role("stardelt-analyst")
        add, remove = converge(names(role), want, grants([d.perm for d in trino], [t.perm for t in tables]))
        for n, v in add:
            pv = sm.find_permission_view_menu(n, v)
            if pv:
                sm.add_permission_role(role, pv)
        for n, v in remove:
            sm.del_permission_role(role, sm.find_permission_view_menu(n, v))   # drift: the role is exactly this set
        sm.get_session.commit()
        print("role stardelt-analyst:", len(role.permissions), "permissions,", len(trino), "Trino database(s),", len(tables), "dataset(s)")


if __name__ == "__main__":
    main()
