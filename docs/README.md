---
layout: default
title: Documentation README
nav_exclude: true
search_exclude: true
sitemap: false
---

# SQL Testing Library Documentation

This directory contains the documentation for the SQL Testing Library, built with Jekyll and hosted on GitHub Pages.

## Documentation Structure

```
docs/
├── _config.yml          # Jekyll configuration
├── index.md            # Homepage
├── getting-started.md  # Installation and quick start guide
├── adapters.md         # Database adapter documentation
├── api-reference.md    # Complete API reference
├── examples.md         # Code examples and patterns
├── troubleshooting.md  # Common issues and solutions
├── migration.md        # Version migration guide
└── assets/
    └── css/
        └── custom.css  # Custom styling
```

## Viewing Documentation

### Online
Visit: https://gurmeetsaran.github.io/sqltesting/

### Locally

#### Option 1: Using Docker (recommended - no Ruby required)
```bash
# Navigate to docs directory
cd docs

# Run Jekyll in Docker (works on ARM64/M1 Macs)
docker run --rm -v "$PWD":/site -p 4000:4000 bretfisher/jekyll-serve

# View at http://localhost:4000/sqltesting/

# Note: Initial run will take a few minutes to install dependencies
# Subsequent runs will be much faster
```

#### Option 2: Using Ruby (if you have Ruby 2.7+ installed)
```bash
# Navigate to docs directory
cd docs

# Install bundler compatible with your Ruby version
gem install bundler -v 2.4.22

# Install dependencies
bundle install

# Serve locally
bundle exec jekyll serve

# View at http://localhost:4000/sqltesting/
```

#### Option 3: Just push to GitHub (easiest)
The easiest way is to push your changes and let GitHub Pages build it automatically. No local setup required!

## Contributing to Documentation

1. Edit the markdown files in this directory
2. Follow the existing structure and formatting
3. Test locally before submitting PR
4. Keep examples practical and tested

## Documentation Guidelines

- Use clear, concise language
- Include code examples for all features
- Keep navigation structure flat and simple
- Test all code examples
- Update when adding new features

## Jekyll Theme

Using [Just the Docs](https://github.com/pmarsceill/just-the-docs) theme for clean, searchable documentation.
