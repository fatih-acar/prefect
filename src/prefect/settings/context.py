from contextlib import contextmanager
from functools import cache
from typing import TYPE_CHECKING, Any, Generator, Iterable, Mapping, Optional

from prefect.settings.models.root import Settings

if TYPE_CHECKING:
    from prefect.context import SettingsContext
    from prefect.settings._types import SettingAccessor
    from prefect.settings.legacy import Setting

    SettingKey = Setting | SettingAccessor


@cache
def _settings_context_type() -> type["SettingsContext"]:
    # Imported once, since `prefect.context` imports this module: an import on
    # every call also raises an `AttributeError` in that module's `__getattr__`,
    # which the import machinery asks for `__path__`.
    from prefect.context import SettingsContext

    return SettingsContext


def get_current_settings() -> Settings:
    """
    Returns a settings object populated with values from the current settings context
    or, if no settings context is active, the environment.
    """
    settings_context = _settings_context_type().get()
    if settings_context is not None:
        return settings_context.settings

    return Settings()


@contextmanager
def temporary_settings(
    updates: Optional[Mapping["SettingKey", Any]] = None,
    set_defaults: Optional[Mapping["SettingKey", Any]] = None,
    restore_defaults: Optional[Iterable["SettingKey"]] = None,
) -> Generator[Settings, None, None]:
    """
    Temporarily override the current settings by entering a new profile.

    See `Settings.copy_with_update` for details on different argument behavior.

    Examples:

        ```python
        from prefect.settings import PREFECT_API_URL

        with temporary_settings(updates={PREFECT_API_URL: "foo"}):
           assert PREFECT_API_URL.value() == "foo"

           with temporary_settings(updates={"api.url": "bar"}):
                assert get_current_settings().api.url == "bar"

           with temporary_settings(set_defaults={PREFECT_API_URL: "bar"}):
                assert PREFECT_API_URL.value() == "foo"

           with temporary_settings(restore_defaults={PREFECT_API_URL}):
                assert PREFECT_API_URL.value() is None

                with temporary_settings(set_defaults={PREFECT_API_URL: "bar"})
                    assert PREFECT_API_URL.value() == "bar"
        assert PREFECT_API_URL.value() is None
        ```
    """
    import prefect.context

    context = prefect.context.get_settings_context()

    if not restore_defaults:
        restore_defaults = []

    new_settings = context.settings.copy_with_update(
        updates=updates, set_defaults=set_defaults, restore_defaults=restore_defaults
    )

    with prefect.context.SettingsContext(
        profile=context.profile, settings=new_settings
    ):
        yield new_settings
