# Security Policy

## Supported Versions

We actively support the following versions of SQL Testing Library with security updates:

| Version | Supported          |
| ------- | ------------------ |
| 0.17.x  | :white_check_mark: |
| 0.16.x  | :white_check_mark: |
| < 0.16  | :x:                |

## Reporting a Vulnerability

We take the security of SQL Testing Library seriously. If you believe you have found a security vulnerability, please report it to us as described below.

### Please do NOT:

- Open a public GitHub issue for security vulnerabilities
- Disclose the vulnerability publicly before we've had a chance to address it

### Please DO:

**Report security vulnerabilities via email to:** gurmeetx@gmail.com

Please include the following information in your report:

- Type of vulnerability (e.g., SQL injection, authentication bypass, etc.)
- Full paths of source file(s) related to the manifestation of the vulnerability
- The location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

### What to expect:

- **Acknowledgment**: We will acknowledge receipt of your vulnerability report within 48 hours
- **Investigation**: We will investigate and validate the vulnerability
- **Updates**: We will keep you informed of our progress
- **Resolution**: We will work on a fix and coordinate disclosure timing with you
- **Credit**: We will credit you in the security advisory (unless you prefer to remain anonymous)

## Security Best Practices

When using SQL Testing Library:

1. **Credentials Management**
   - Never commit database credentials to version control
   - Use environment variables or secure credential management systems
   - Rotate credentials regularly

2. **Test Data**
   - Avoid using production data in tests
   - Sanitize any sensitive data used in examples
   - Use mock data generators for realistic test data

3. **Dependency Management**
   - Keep dependencies up to date
   - Regularly run security audits: `pip audit` or `safety check`
   - Monitor security advisories for database drivers

4. **Database Connections**
   - Use encrypted connections (TLS/SSL) when available
   - Limit network access to test databases
   - Use least-privilege service accounts for testing

5. **CI/CD Security**
   - Secure GitHub Actions secrets properly
   - Use OIDC authentication instead of long-lived credentials when possible
   - Enable branch protection and require reviews

## Security Advisories

We will publish security advisories through:

- [GitHub Security Advisories](https://github.com/gurmeetsaran/sqltesting/security/advisories)
- Release notes on [GitHub Releases](https://github.com/gurmeetsaran/sqltesting/releases)
- PyPI release notes

## Scope

This security policy applies to:

- The SQL Testing Library core package
- Official database adapters (BigQuery, Snowflake, Redshift, Athena, Trino, DuckDB)
- Documentation and examples

It does NOT apply to:

- Third-party integrations or forks
- Community-contributed plugins
- Issues in upstream database drivers (report those to respective projects)

## Known Limitations

Please be aware of the following security considerations:

1. **SQL Injection Prevention**: While this library helps test SQL queries, it does not prevent SQL injection in your application code. Always use parameterized queries in production.

2. **Temporary Tables**: Some adapters create temporary tables or S3 objects. Ensure proper cleanup and access controls.

3. **Query Logging**: SQL queries may be logged. Avoid including sensitive data in query comments or literals.

## Compliance

SQL Testing Library is designed to be used in compliance-conscious environments. If you have specific compliance requirements (GDPR, HIPAA, SOC 2, etc.), please review our documentation and contact us if you have questions.

## Contact

For security-related questions or concerns:
- Email: gurmeetx@gmail.com
- GitHub: [@gurmeetsaran](https://github.com/gurmeetsaran)

For general questions and discussions:
- GitHub Discussions: https://github.com/gurmeetsaran/sqltesting/discussions
- GitHub Issues: https://github.com/gurmeetsaran/sqltesting/issues

Thank you for helping keep SQL Testing Library and our community safe!
