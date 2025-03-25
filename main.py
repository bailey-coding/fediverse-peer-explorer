#!/usr/bin/env python3
import asyncio
import sqlite3
from dataclasses import dataclass
from datetime import datetime, UTC, timedelta
from typing import Optional

import aiohttp
from aiohttp import ClientSession, ClientTimeout
from aiohttp_client_cache import CachedSession, SQLiteBackend
from tabulate import tabulate
from tqdm import tqdm


@dataclass
class FediverseInstance:
    domain: str
    last_updated_at: float
    software: Optional[str] = ""
    version: Optional[str] = ""
    error: Optional[str] = ""


async def fetch(
    session: ClientSession, peer: str, semaphore, progress: Optional[tqdm] = None
) -> FediverseInstance:
    async with semaphore:
        try:
            async with session.get(f"https://{peer}/.well-known/nodeinfo") as response:
                if progress:
                    progress.update(1)
                # print(f'Fetching data for {peer}')
                try:
                    node_info_links = await response.json()
                    links = [
                        link
                        for link in node_info_links["links"]
                        if link["rel"]
                        in (
                            "https://nodeinfo.diaspora.software/ns/schema/1.0",
                            "http://nodeinfo.diaspora.software/ns/schema/2.0",
                            "http://nodeinfo.diaspora.software/ns/schema/2.1",
                        )
                    ]
                    if len(links) == 0:
                        raise Exception(node_info_links["links"])
                    node_info = await (
                        await session.get(node_info_links["links"][0]["href"])
                    ).json()
                    return FediverseInstance(
                        peer,
                        last_updated_at=datetime.now(UTC).timestamp(),
                        software=node_info["software"]["name"],
                        version=node_info["software"]["version"],
                    )
                except aiohttp.ClientError as e:
                    return FediverseInstance(
                        peer,
                        last_updated_at=datetime.now(UTC).timestamp(),
                        error=str(e)[:40],
                    )
        except Exception as e:
            if any(i in peer for i in ("pix", "photo")):
                print(f"Error fetching {peer}: {type(e)} {e}")
            return FediverseInstance(
                peer,
                last_updated_at=datetime.now(UTC).timestamp(),
                error=f"Error fetching {peer}: {type(e)} {e}",
            )


async def main():
    con = sqlite3.connect("db.sqlite")
    con.row_factory = sqlite3.Row
    with con:
        cur = con.cursor()
        cur.execute(
            """SELECT rowid, domain, software_name, software_version, error, last_updated_at FROM instances"""
        )
        existing_rows = cur.fetchall()
    existing_domains = {row["domain"]: row for row in existing_rows}
    results = []
    async with CachedSession(
        cache=SQLiteBackend("demo_cache"),
        timeout=ClientTimeout(total=15, sock_connect=15),
    ) as session:
        peers = ["hachyderm.io"]
        peers.extend(
            await (
                await session.get("https://hachyderm.io/api/v1/instance/peers")
            ).json()
        )
        # peers = [
        #     'pixelfed.social'
        # ]
        progress = None
        semaphore = asyncio.Semaphore(50)
        # progress = tqdm(total=len(peers))
        tasks = []
        one_day_ago = datetime.now(UTC) - timedelta(days=1)
        fetched_peers = []
        for p in peers:
            if p in existing_domains:
                data = existing_domains[p]
                if (
                    "DNSError" in data["error"]
                    or datetime.fromtimestamp(data["last_updated_at"], tz=UTC)
                    > one_day_ago
                ):
                    results.append(
                        FediverseInstance(
                            domain=data["domain"],
                            last_updated_at=data["last_updated_at"],
                            software=data["software_name"],
                            version=data["software_version"],
                            error=data["error"],
                        )
                    )
                    continue
            fetched_peers.append(p)
            tasks.append(fetch(session, p, semaphore, progress))
        task_results = await asyncio.gather(
            *tasks,
            return_exceptions=True,  # default is false, that would raise
        )
        # progress.close()
        for idx, p in enumerate(fetched_peers):
            if not any(i in p for i in ("pix", "photo")):
                continue
            print(idx, end=": ")
            if isinstance(task_results[idx], Exception):
                print("{}: {} - {}".format(p, "ERR", task_results[idx]))
            else:
                print("{}: {}".format(p, "OK"))
                results.append(task_results[idx])
    with con:
        con.executemany(
            """INSERT INTO instances
                   (domain, software_name, software_version, error, last_updated_at)
               VALUES (:domain, :software, :version, :error, :last_updated_at)
               ON CONFLICT (domain)
                   DO UPDATE
                   SET software_name=excluded.software_name,
                       software_version=excluded.software_version,
                       error=excluded.error,
                       last_updated_at=excluded.last_updated_at""",
            (r.__dict__ for r in results),
        )
    # TODO: This only prints the results, and not the ones we didn't fetch because they were already in the database.
    print(
        tabulate(
            sorted(
                (r.__dict__ for r in results if r.software == "pixelfed"),
                key=lambda x: x["version"],
            ),
            headers={
                k: k
                for k in ("domain", "last_updated_at", "software", "version", "error")
            },
            tablefmt="github",
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
