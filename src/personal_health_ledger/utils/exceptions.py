"""Custom exceptions for the personal health ledger."""


class PersonalHealthLedgerError(Exception):
    """Base exception for all personal health ledger errors."""

    pass


class ConfigurationError(PersonalHealthLedgerError):
    """Raised when there is a configuration error."""

    pass


class AuthenticationError(PersonalHealthLedgerError):
    """Raised when authentication fails."""

    pass


class DriveClientError(PersonalHealthLedgerError):
    """Raised when Drive API operations fail."""

    pass


class ParsingError(PersonalHealthLedgerError):
    """Raised when file parsing fails."""

    pass


class ConsolidationError(PersonalHealthLedgerError):
    """Raised when data consolidation fails."""

    pass


class ValidationError(PersonalHealthLedgerError):
    """Raised when data validation fails."""

    pass
