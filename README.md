# Fediverse Peer Explorer

An easy way to find out a little more information about the peers of your Fediverse instance. Primarily what software they're running and what version it is on.

This originally was created to understand what peers were running pixelfed versions earlier than 0.12.5

## Quickstart

To setup Python, necessary dependencies, etc.

```
uv sync
```

To just print pixelfed instances and their version

```
uv run fpe query
```

To update the DB

```
uv run fpe update hachyderm.io
```

If you don't want to run this, and just want to see an example of the data that might be outdated see [EXAMPLE_DATA.md](./EXAMPLE_DATA.md)

## Contributing

Issues and PRs welcome!

This should have tests.

## Roadmap

See the GitHub issues.