# Contributing to Databricks Calendar Dimension

Thank you for your interest in contributing to this project! This document provides guidelines and information for contributors.

## How to Contribute

### Reporting Bugs

Before creating a bug report, please check existing issues to avoid duplicates. When creating a bug report, include:

- A clear, descriptive title
- Steps to reproduce the issue
- Expected behavior vs actual behavior
- Your environment (Python version, Databricks runtime version, etc.)
- Any relevant logs or error messages

### Suggesting Enhancements

Enhancement suggestions are welcome! Please include:

- A clear description of the enhancement
- The motivation and use case
- Any potential implementation approaches

### Pull Requests

1. **Fork the repository** and create your branch from `main`
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Make your changes** following the coding standards below
4. **Add tests** for any new functionality
5. **Run the test suite**:
   ```bash
   pytest tests/ -v
   ```
6. **Format your code**:
   ```bash
   black src/ tests/
   flake8 src/ tests/
   ```
7. **Commit your changes** with a clear commit message
8. **Push to your fork** and submit a pull request

## Coding Standards

### Python Style

- Follow [PEP 8](https://peps.python.org/pep-0008/) style guidelines
- Use [Black](https://github.com/psf/black) for code formatting
- Use type hints where appropriate
- Maximum line length: 88 characters (Black default)

### Code Quality

- Write clear, self-documenting code
- Add docstrings to functions and classes
- Keep functions focused and concise
- Handle errors gracefully

### Testing

- Write tests for new functionality
- Maintain or improve code coverage
- Tests should be deterministic and not depend on external services
- Use descriptive test names

### Commit Messages

- Use clear, descriptive commit messages
- Start with a verb in present tense (e.g., "Add", "Fix", "Update")
- Reference issues when applicable (e.g., "Fix #123")

## Development Setup

1. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/Calendar-API.git
   cd Calendar-API
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Run tests to verify setup:
   ```bash
   pytest tests/ -v
   ```

## Project Structure

```
Calendar-API/
├── src/                    # Source code
│   ├── generate_dim_calendar.py  # Main calendar generation
│   └── utils/              # Utility functions
├── tests/                  # Test suite
├── sql/                    # SQL scripts
├── notebooks/              # Databricks notebooks
├── docs/                   # Documentation
└── config/                 # Configuration files
```

## Questions?

If you have questions about contributing, feel free to open an issue for discussion.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
