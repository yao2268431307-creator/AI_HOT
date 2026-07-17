"""DBA-only provisioning for fixed least-privilege production identities."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import psycopg
from psycopg import sql


ROLE_SPECS = {
    "radar_app": "app_password_file",
    "radar_deletion_worker": "deletion_password_file",
}


def read_secret(path: Path) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if len(value) < 20:
        raise ValueError(f"{path.name} must contain at least 20 characters")
    return value


def connection_secret(direct: str | None, path: Path | None) -> str:
    if bool(direct) == bool(path):
        raise ValueError("provide exactly one of --admin-dsn or --admin-dsn-file")
    if direct is not None:
        return direct
    assert path is not None
    return read_secret(path)


def ensure_database(connection: psycopg.Connection[object]) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT current_database(), current_user, role.rolsuper, role.rolcreaterole,
                   pg_get_userbyid(database.datdba) = current_user AS owns_database
            FROM pg_roles AS role
            JOIN pg_database AS database ON database.datname = current_database()
            WHERE role.rolname = current_user
            """
        )
        database, user, superuser, create_role, owns_database = cursor.fetchone()
    if database != "ai_hot":
        raise RuntimeError("provisioning must run against the ai_hot database")
    # Managed PostgreSQL services normally do not expose a true superuser. Their
    # bootstrap identity is still sufficient when it can create roles and owns
    # the target database (and therefore the migrated public objects).
    if not superuser and not (create_role and owns_database):
        raise RuntimeError(
            f"{user} must be a superuser or a CREATEROLE owner of the ai_hot database"
        )


def revoke_role_memberships(cursor: psycopg.Cursor[object], role: str) -> None:
    cursor.execute(
        """
        SELECT granted.rolname, member.rolname
        FROM pg_auth_members membership
        JOIN pg_roles AS granted ON granted.oid=membership.roleid
        JOIN pg_roles AS member ON member.oid=membership.member
        WHERE granted.rolname=%s OR member.rolname=%s
        """,
        (role, role),
    )
    for granted_role, member_role in cursor.fetchall():
        cursor.execute(
            sql.SQL("REVOKE {} FROM {}").format(
                sql.Identifier(str(granted_role)), sql.Identifier(str(member_role)),
            )
        )


def provision_runtime_roles(dsn: str, app_password_file: Path, deletion_password_file: Path) -> dict[str, object]:
    passwords = {
        "radar_app": read_secret(app_password_file),
        "radar_deletion_worker": read_secret(deletion_password_file),
    }
    with psycopg.connect(dsn) as connection:
        ensure_database(connection)
        with connection.cursor() as cursor:
            for role, password in passwords.items():
                cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=%s)", (role,))
                if not cursor.fetchone()[0]:
                    cursor.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role)))
                cursor.execute(sql.SQL(
                    "ALTER ROLE {} WITH LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
                ).format(sql.Identifier(role), sql.Literal(password)))
                revoke_role_memberships(cursor, role)
        connection.commit()
    return {"database": "ai_hot", "rolesProvisioned": sorted(passwords)}


def provision_capacity_reader(dsn: str, password_file: Path) -> dict[str, object]:
    password = read_secret(password_file)
    role = "radar_capacity_reader"
    tables = ("sources", "observations", "events", "runtime_component_heartbeats")
    with psycopg.connect(dsn) as connection:
        ensure_database(connection)
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('public.runtime_component_heartbeats')")
            if cursor.fetchone()[0] is None:
                raise RuntimeError("apply infra/postgres/001_init.sql before creating the capacity reader")
            cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=%s)", (role,))
            if not cursor.fetchone()[0]:
                cursor.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role)))
            cursor.execute(sql.SQL(
                "ALTER ROLE {} WITH LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
            ).format(sql.Identifier(role), sql.Literal(password)))
            revoke_role_memberships(cursor, role)
            cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE ai_hot TO {}").format(sql.Identifier(role)))
            cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role)))
            cursor.execute(sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {}").format(sql.Identifier(role)))
            cursor.execute(sql.SQL("GRANT SELECT ON {} TO {}").format(
                sql.SQL(",").join(sql.Identifier(table) for table in tables), sql.Identifier(role),
            ))
        connection.commit()
    return {"database": "ai_hot", "capacityReader": role, "selectTables": list(tables)}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Provision AI Hot Radar production database identities")
    commands = result.add_subparsers(dest="command", required=True)
    roles = commands.add_parser("runtime-roles")
    roles_dsn = roles.add_mutually_exclusive_group(required=True)
    roles_dsn.add_argument("--admin-dsn")
    roles_dsn.add_argument("--admin-dsn-file", type=Path)
    roles.add_argument("--app-password-file", type=Path, required=True)
    roles.add_argument("--deletion-password-file", type=Path, required=True)
    reader = commands.add_parser("capacity-reader")
    reader_dsn = reader.add_mutually_exclusive_group(required=True)
    reader_dsn.add_argument("--admin-dsn")
    reader_dsn.add_argument("--admin-dsn-file", type=Path)
    reader.add_argument("--password-file", type=Path, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    dsn = connection_secret(args.admin_dsn, args.admin_dsn_file)
    if args.command == "runtime-roles":
        payload = provision_runtime_roles(dsn, args.app_password_file, args.deletion_password_file)
    else:
        payload = provision_capacity_reader(dsn, args.password_file)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
