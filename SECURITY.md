# Security Policy

## Supported Versions

We release patches for security vulnerabilities. Which versions are eligible for receiving such patches depends on the CVSS v3.0 Rating:

| Version | Supported          | Status |
| ------- | ------------------ | ------ |
| latest  | ✅ | Active development |
| < latest | ❌ | Security fixes only for critical issues |

## Reporting a Vulnerability

We take the security of cereal seriously. If you have discovered a security vulnerability in this project, please report it responsibly.

### How to Report

**Please DO NOT report security vulnerabilities through public GitHub issues.**

Instead, please report them via one of the following methods:

1. **GitHub Security Advisories** (Preferred)
   - Go to the [Security tab](https://github.com/zoobzio/cereal/security) of this repository
   - Click "Report a vulnerability"
   - Fill out the form with details about the vulnerability

2. **Email**
   - Send details to the repository maintainer through GitHub profile contact information
   - Use PGP encryption if possible for sensitive details

### What to Include

Please include the following information (as much as you can provide) to help us better understand the nature and scope of the possible issue:

- **Type of issue** (e.g., SQL injection, buffer overflow, access control bypass, etc.)
- **Full paths of source file(s)** related to the manifestation of the issue
- **The location of the affected source code** (tag/branch/commit or direct URL)
- **Any special configuration required** to reproduce the issue
- **Step-by-step instructions** to reproduce the issue
- **Proof-of-concept or exploit code** (if possible)
- **Impact of the issue**, including how an attacker might exploit the issue
- **Your name and affiliation** (optional)

### What to Expect

- **Acknowledgment**: We will acknowledge receipt of your vulnerability report within 48 hours
- **Initial Assessment**: Within 7 days, we will provide an initial assessment of the report
- **Resolution Timeline**: We aim to resolve critical issues within 30 days
- **Disclosure**: We will coordinate with you on the disclosure timeline

### Preferred Languages

We prefer all communications to be in English.

## Security Best Practices

When using cereal in your applications, we recommend:

1. **Keep Dependencies Updated**
   ```bash
   go get -u github.com/zoobzio/cereal
   ```

2. **Use Context Properly**
   - Always pass contexts with appropriate timeouts
   - Handle context cancellation in your queries

3. **Input Validation**
   - Validate all user inputs before passing to queries
   - Use parameterized queries (cereal handles this automatically)
   - Never construct raw SQL from user input

4. **Error Handling**
   - Never expose internal error details to users
   - Log errors securely without leaking sensitive data
   - Implement proper fallback mechanisms

5. **Database Security**
   - Use least-privilege database accounts
   - Enable SSL/TLS for database connections
   - Rotate database credentials regularly
   - Never commit credentials to version control

6. **SQL Injection Protection**
   - cereal uses parameterized queries via sqlx
   - All user inputs are properly escaped
   - ASTQL validates query structure at initialization

7. **Schema Validation**
   - Schema validation happens at initialization via ASTQL
   - Invalid queries fail fast before reaching the database
   - Column and table names are validated against schema

## Security Features

cereal includes several built-in security features:

- **Type Safety**: Generic types prevent type confusion attacks
- **SQL Validation**: ASTQL validates all queries against schema
- **Parameterized Queries**: All values use SQL parameters, not string concatenation
- **Context Support**: Built-in timeout and cancellation support
- **Error Isolation**: Errors are properly wrapped without leaking sensitive data
- **Zero SQL Injection Risk**: Query structure is validated; values are parameterized

## Automated Security Scanning

This project uses:

- **CodeQL**: GitHub's semantic code analysis for security vulnerabilities
- **Dependabot**: Automated dependency updates
- **golangci-lint**: Static analysis including security linters (gosec)
- **Codecov**: Coverage tracking to ensure security-critical code is tested

## Known Security Considerations

### SQL Injection

cereal is designed to prevent SQL injection by:
1. Validating query structure at initialization
2. Using parameterized queries for all values
3. Never concatenating user input into SQL strings

### Access Control

cereal does not implement access control - this is the responsibility of:
1. Your application layer
2. Database-level permissions
3. Row-level security policies

### Data Exposure

- Be careful with error messages in production
- Don't log sensitive query parameters
- Implement proper audit logging at application level

## Vulnerability Disclosure Policy

- Security vulnerabilities will be disclosed via GitHub Security Advisories
- We follow a 90-day disclosure timeline for non-critical issues
- Critical vulnerabilities may be disclosed sooner after patches are available
- We will credit reporters who follow responsible disclosure practices

## Credits

We thank the following individuals for responsibly disclosing security issues:

_This list is currently empty. Be the first to help improve our security!_

---

**Last Updated**: 2025-11-04
