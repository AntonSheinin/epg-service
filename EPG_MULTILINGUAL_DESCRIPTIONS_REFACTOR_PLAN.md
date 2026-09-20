# Multilingual EPG Descriptions Refactor Plan

## Status

Implementation-ready service plan. This document covers only `epg_service`. Android application changes are intentionally out of scope.

## Goal

Preserve every usable XMLTV `<desc>` value belonging to a single `<programme>`, then return one description selected according to the optional language requested by the client.

If the preferred-language description is absent or empty, the service must return the next usable description in the programme's original XML order. If no usable description exists, it must return `null`.

## Scope

Included:

- Parse all `<desc>` children of each XMLTV `<programme>`.
- Preserve their order and language metadata.
- Store the descriptions in PostgreSQL.
- Accept an optional preferred description language in `POST /epg`.
- Select one non-empty description for the existing response field.
- Migrate existing description data.
- Add focused parser, selector, schema, repository-boundary, and query tests.

Excluded:

- Android application changes.
- Multilingual programme titles or channel display names.
- Combining descriptions from different configured XMLTV sources.
- Changing current source precedence or programme identity.
- Returning all descriptions through the public API.
- Adding a language-detection library or other dependency.
- Adding general XML namespace support.

## Confirmed Existing Behavior

- `xmltv_parser.py` currently reads only `programme.find("desc")` through `_get_text`, so only the first `<desc>` is retained.
- `Program`, `ProgramRecord`, and the `programs` table currently contain one nullable text `description`.
- Sources are imported sequentially. When the same programme ID occurs again, the later source updates the existing row.
- `POST /epg` currently returns one nullable `description` per programme.
- Alembic migrations run before the service starts.
- The test suite uses the standard-library `unittest` runner and has no permanent PostgreSQL integration-test fixture.
- An audit of all three currently configured XMLTV feeds found that description languages are encoded with lowercase two-letter codes. The client-relevant values are `ru`, `en`, and `he`; no `rus`, `eng`, `heb`, `iw`, or regional tags such as `en-US` were present.
- One configured feed omits the `lang` attribute from all usable descriptions. Those descriptions remain available as document-order fallbacks.

## Final Data Model

### Domain model

Add a small immutable internal value type in `app/models.py`:

```python
@dataclass(frozen=True, slots=True)
class LocalizedDescription:
    language: str | None
    text: str
```

Replace `Program.description` with:

```python
descriptions: tuple[LocalizedDescription, ...] = ()
```

A tuple makes source order explicit and prevents accidental mutation after parsing.

Export `LocalizedDescription` alongside `Channel` and `Program`.

### Database model

Replace `programs.description TEXT NULL` with:

```text
programs.descriptions JSONB NOT NULL DEFAULT '[]'::jsonb
```

The stored JSON shape is an ordered array:

```json
[
  {"language": "ru", "text": "Описание"},
  {"language": "en", "text": "Description"},
  {"language": null, "text": "Unlabelled fallback"}
]
```

Do not create an index for this column. Selection happens after programmes are loaded, and no query filters by description language.

Do not retain both database columns after migration. Keeping two authoritative description representations would create unnecessary synchronization and fallback logic.

## XMLTV Parsing Rules

In `_parse_single_program`, replace the singular `_get_text(programme, "desc")` call with a helper that iterates over `programme.findall("desc")`.

For every `<desc>` in XML order:

1. Obtain visible text with `"".join(element.itertext())`.
2. Trim surrounding whitespace.
3. Skip the entry if the trimmed text is empty.
4. Read the optional `lang` attribute.
5. Normalize a non-empty language value by:
   - trimming whitespace;
   - converting it to lowercase;
   - replacing `_` with `-`.
6. Convert a missing or blank language value to `None`.
7. Append `LocalizedDescription(language, text)` without reordering or deduplicating it.

Duplicate languages must be preserved because the first exact match in XML order is authoritative. Descriptions from separate `<programme>` elements or separate source files must never be pooled together by the parser.

The existing `_get_text` helper remains in use for singular fields such as `title` and `display-name`.

## Language Request Contract

Add the following optional field to `EPGRequest`:

```python
preferred_description_language: Literal["ru", "en", "he"] | None = Field(
    default=None,
    description="Preferred language for programme descriptions",
)
```

`Literal` is already imported in `app/schemas.py`. Do not add a language-normalization validator or language-tag regex. The accepted non-null values are exactly `ru`, `en`, and `he`; values such as `RU`, `rus`, `eng`, `iw`, `en-US`, blank strings, and strings with surrounding whitespace are rejected by normal Pydantic validation.

Requests that omit the new field remain valid. The response schema remains unchanged.

Example request:

```json
{
  "channels": [{"xmltv_id": "channel1"}],
  "timezone": "Asia/Jerusalem",
  "from_date": "2026-09-20T00:00:00Z",
  "to_date": "2026-09-21T00:00:00Z",
  "preferred_description_language": "ru"
}
```

## Description Selection and Fallback

Implement a pure helper in `app/services/epg_query.py`, or in one small adjacent module if keeping it there would make the file unclear:

```python
def select_description(
    descriptions: Sequence[LocalizedDescription],
    preferred_language: str | None,
) -> str | None:
    ...
```

Use this exact selection algorithm:

1. Defensively ignore entries whose trimmed text is empty.
2. If a preferred language exists, scan in stored XML order and return the first usable entry whose stored language exactly equals it.
3. If no exact preferred-language match exists, return the first usable entry in stored XML order.
4. If no usable entry exists, return `None`.

An unlabelled entry does not receive special priority. It participates in the normal document-order fallback.

Do not implement aliases, primary-subtag matching, regional matching, translation, or automatic language detection. Supporting another preferred language later requires an explicit API-contract change to the `Literal` values and corresponding tests.

## Database Migration

Create Alembic revision `0006` with `down_revision = "0005_import_status_sources_count"`.

### Upgrade

1. Import and use PostgreSQL `JSONB`, then add nullable `descriptions JSONB` temporarily.
2. Backfill rows:
   - non-null, non-blank `description` becomes an ordered single-entry array with `language: null`;
   - null or blank `description` becomes `[]`.
   - use `description !~ '^[[:space:]]*$'` (together with the null check) for the non-blank branch so spaces, tabs, carriage returns, and newlines are treated as blank;
   - use PostgreSQL JSON constructors or an equivalently parameter-safe expression rather than assembling JSON text manually.
3. Alter `descriptions` to `nullable=False` and set an explicitly typed server default with `sa.text("'[]'::jsonb")`.
4. Drop the old `description` column.

Using a temporarily nullable column avoids relying on a default while existing rows are converted.

### Downgrade

1. Add nullable `description TEXT`.
2. Populate it with the `text` value of the first JSON-array entry only when:
   - `jsonb_typeof(descriptions) = 'array'`;
   - the array is non-empty;
   - the first entry is an object; and
   - its `text` member is a string that remains non-empty after trimming ordinary whitespace.
3. Store the trimmed first-entry text. Leave `description` as `NULL` for an empty array, a missing or non-string `text` member, a whitespace-only `text` value, or any other valid but unexpected JSONB shape.
4. Remove the `descriptions` server default explicitly, then drop `descriptions`.

JSONB cannot contain syntactically malformed JSON, but it can contain an unexpected valid shape; the downgrade must tolerate those shapes. The downgrade necessarily and permanently loses every description after the first, which matches the old schema's capabilities.

## ORM and Repository Changes

In `app/db/orm_models.py`:

- Import PostgreSQL `JSONB`.
- Import SQLAlchemy `text` for the typed server-default expression.
- Replace `ProgramRecord.description` with `ProgramRecord.descriptions`.
- Use `server_default=text("'[]'::jsonb")` and `nullable=False`.

In `app/db/repository.py`:

- Replace `"description"` with `"descriptions"` in `_PROGRAM_INSERT_COLUMNS`.
- Serialize each `LocalizedDescription` to `{ "language": ..., "text": ... }` while constructing insert rows.
- Update `descriptions` in `ON CONFLICT DO UPDATE`.
- Add `ProgramRecord.descriptions.is_distinct_from(stmt.excluded.descriptions)` to the update condition.
- Deserialize database JSON into an ordered tuple when creating domain `Program` values.

Keep serialization and deserialization in small pure helpers so the JSON boundary can be unit tested without PostgreSQL.

Deserialization must tolerate unexpected stored data without failing the entire `/epg` request:

- A non-list value becomes an empty tuple.
- Non-dictionary entries are skipped.
- Entries without string text are skipped.
- Text is trimmed and empty text is skipped.
- A string language is normalized with the same trim, lowercase, and underscore-to-hyphen rules as parsed XML; a non-string or blank language becomes `None`.

This defensive read is limited to the JSON boundary and must not introduce a second legacy-description fallback.

Current source overwrite behavior remains unchanged: a later source replaces the complete `descriptions` array for a matching programme ID.

## Query Response Changes

In `app/services/epg_query.py`, replace:

```python
description=p.description
```

with selection from `p.descriptions` and `request.preferred_description_language`.

Keep `ProgramResponse.description: str | None` unchanged. Do not expose language metadata or the complete array in the public response.

The request log may include the validated preferred language, but it must not log description text.

## Focused Tests

Use the existing standard-library `unittest` structure.

### Parser tests

Add cases proving that:

- multiple non-empty descriptions are preserved in XML order;
- `lang` is normalized;
- missing or blank `lang` becomes `None`;
- empty and whitespace-only descriptions are skipped;
- nested text is collected with `itertext()`;
- duplicate language entries remain ordered.

### Selector tests

Add cases proving that:

- exact `ru`, `en`, and `he` entries can each be selected;
- XML order decides between duplicate exact-language entries;
- an alias or regional tag does not match a requested language;
- an absent preferred language returns the first usable description;
- a supported preferred language that is unavailable for the programme falls back to the first usable description;
- no usable descriptions returns `None`.

### Schema tests

Add cases for:

- omitted and null preference;
- each exact value `ru`, `en`, and `he` being accepted;
- unsupported strings, including aliases and regional tags, being rejected;
- uppercase, surrounding whitespace, and blank strings being rejected; and
- a non-string value being rejected by normal Pydantic validation.

### Repository-boundary tests

Test the pure serialization and deserialization helpers without a database fixture. Add cases proving that:

- serialization preserves array order and emits only `language` and `text`;
- deserialization preserves valid entry order;
- a non-list top-level value becomes an empty tuple;
- non-dictionary array entries are skipped;
- entries with missing or non-string text are skipped;
- text is trimmed and whitespace-only text is skipped; and
- a valid string language is normalized consistently with parser output; and
- a missing, non-string, or blank language becomes `None`.

### Query test

Use a small fake repository to verify that `get_epg_data` returns the selected singular description. One query-level test is enough because the selection matrix belongs to the pure-helper tests.

Do not add a new testing dependency or permanent PostgreSQL fixture.

## Validation

Run:

```powershell
.\.venv\Scripts\python.exe -m compileall app
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Then perform one Docker/PostgreSQL smoke test:

1. Start from revision `0005` with rows containing non-empty, space-only, tab/newline-only, and null descriptions.
2. Upgrade to `0006`.
3. Verify JSON backfill and the absence of the old column.
4. Run one import containing multiple description languages.
5. Query `/epg` with a supported preference present in the programme, a supported preference absent from the programme, and a null preference; also verify that an unsupported preference receives a validation error.
6. Before downgrade, insert or update representative valid-but-unexpected JSONB shapes, including a first entry with whitespace-only `text`, and verify that downgrade handles them without failing.
7. Downgrade to `0005` and verify that trimmed first-entry text is restored for a valid array, whitespace-only text becomes `NULL`, and later descriptions are discarded.

## Documentation

Update `README.md` to document:

- the optional `preferred_description_language` request field;
- its exact accepted values (`ru`, `en`, and `he`) and rejection of every other value;
- exact-match and document-order fallback behavior;
- the unchanged singular `description` response field;
- the fact that language metadata is populated only by a fresh XMLTV import.

Update the documented database programme fields from `description` to `descriptions`.

## Deployment Sequence

1. Implement and validate the parser, model, migration, repository, request schema, selector, and tests together.
2. Deploy the service; the entrypoint applies migration `0006` before starting the new code.
3. Trigger a complete EPG fetch after deployment.
4. Verify several known multilingual programmes through `/epg` using at least two supported preferred languages and one supported language that is absent from the selected programme.

Immediately after migration but before the fresh fetch, old descriptions remain available as unlabelled fallbacks. Accurate preferred-language matching begins as programmes are reimported with their XMLTV language metadata.

The old application binary is incompatible with the upgraded schema because `programs.description` no longer exists. If deployment must be rolled back:

1. Stop the new service so it cannot write while the schema is changing.
2. Run `alembic downgrade 0005_import_status_sources_count` using the release that contains migration `0006`.
3. Confirm that `programs.description` has been restored and `programs.descriptions` has been removed.
4. Start the previous application version.

Do not start the previous application against revision `0006`. A rollback downgrade permanently discards every description after the first, so take an operational database backup first when retaining the newly imported multilingual data matters.

## Acceptance Criteria

The service refactor is complete when all of the following are true:

- Every non-empty `<desc>` within a programme is stored in XML order with normalized language metadata.
- Empty descriptions never prevent selection of a later usable description.
- The request accepts only `ru`, `en`, `he`, or `null`/omission.
- A requested language selects the first exact match in XML order; aliases and language-family matching are not performed.
- An omitted preference, or a supported preferred language unavailable for the programme, falls back to the first usable description.
- `/epg` still returns one nullable `description` string per programme.
- Multiple XMLTV sources retain their current overwrite behavior.
- Migration upgrade and downgrade work with existing data, ordinary whitespace-only legacy values, and valid-but-unexpected JSONB shapes.
- A fresh import populates real description-language metadata.
- The existing and newly added unit tests pass without new dependencies.
