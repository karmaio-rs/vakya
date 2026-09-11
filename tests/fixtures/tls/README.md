# TLS test fixtures

These hexadecimal DER fixtures define a test-only P-256 certificate authority,
a `localhost` server certificate (also valid for `127.0.0.1`), and its PKCS#8
private key. They are valid from 2026-09-04 through 2036-09-01 and must never be
used outside automated tests.

The integration test decodes the text at runtime. Keeping fixtures textual
avoids a certificate parsing dependency in Vakya's production feature graph.
