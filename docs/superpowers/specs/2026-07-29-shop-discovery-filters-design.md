# Shop Discovery and Filters Design

## Context

The shop list page always sends hard-coded Hangzhou coordinates. This selects
the Redis GEO query path, but the Docker deployment never runs the test-only
`loadShopData()` initializer, so `shop:geo:{typeId}` keys are absent and the
page is empty. The visible area and sorting controls are also incomplete:
only the shop type is a real dropdown, while popularity and score are not
implemented by the backend.

## Goals

- Populate the Redis GEO index automatically and safely from MySQL.
- Support composable shop type, area, distance, popularity, and score filters.
- Preserve the tutorial's Redis GEO architecture for nearby-shop queries.
- Make loading, empty, and error states explicit on the mobile shop list page.
- Keep Hangzhou as the only displayed city because the current data model and
  seed data do not contain multiple cities.

## Non-Goals

- Adding a city column or multi-city seed data.
- Implementing a full-text shop search experience.
- Building Redis indexes for popularity, score, or area.
- Changing the existing shop category model.

## API

### Query Shops

`GET /shop/of/type`

Parameters:

| Parameter | Required | Meaning |
| --- | --- | --- |
| `typeId` | yes | Shop type identifier |
| `current` | no | One-based page number, default `1` |
| `sortBy` | no | `distance`, `comments`, or `score`; defaults to `distance` |
| `area` | no | Exact `tb_shop.area` filter |
| `x` | conditional | Longitude, required for distance sorting |
| `y` | conditional | Latitude, required for distance sorting |

Stable ordering:

- `distance`: Redis GEO distance ascending, then shop ID ascending.
- `comments`: comments descending, then shop ID ascending.
- `score`: score descending, then shop ID ascending.
- If distance is requested without coordinates, query MySQL ordered by shop ID
  instead of returning fabricated distance data.

Distance and area filters are composable. The service searches the GEO radius,
loads the matching shops, applies the exact area filter, preserves GEO order,
and paginates the filtered result. This is appropriate for the current small
dataset and keeps the implementation aligned with the tutorial. The query
boundary remains isolated so a larger deployment can later use per-area GEO
indexes without changing the HTTP contract.

Invalid sort values, coordinates, or page numbers return a failed `Result`
with a specific validation message.

### Query Areas

`GET /shop/areas?typeId={typeId}`

Returns distinct, nonblank area names for the requested shop type, ordered
lexicographically. An empty category returns an empty list.

## GEO Index Lifecycle

An application startup component rebuilds GEO data from MySQL:

1. Query all shops with nonnull type and coordinates.
2. Group shops by `typeId`.
3. Write each group to a unique temporary Redis key.
4. Atomically replace `shop:geo:{typeId}` with the completed temporary key.
5. Remove stale GEO keys for types that no longer contain shops.

The initializer is idempotent. A Redis failure is logged clearly but does not
prevent the application from starting. Distance queries return a specific
business failure while GEO is unavailable.

Shop writes keep the index current:

- Saving a shop adds it to the GEO key for its type.
- Updating coordinates updates the member position.
- Changing type removes the member from the old key and adds it to the new key.
- Removing coordinates removes the member from GEO.

Database writes remain authoritative. GEO synchronization runs only after a
successful database write.

## Frontend Interaction

The shop list toolbar contains:

`[Shop Type] [All Areas] [Distance] [Popularity] [Score]`

- Shop type and area are dropdowns.
- Distance, popularity, and score are mutually exclusive sort controls.
- The active sort uses the existing orange accent and an underline.
- Changing type, area, or sort clears the old results, resets the page to `1`,
  and starts a new request.
- Infinite scrolling loads the next page once and prevents duplicate requests.
- An empty response renders a visible empty state.
- A failed request renders its error and offers a retry action.
- Hangzhou remains fixed on the home page and its misleading dropdown arrow is
  removed.
- The unimplemented search icon is noninteractive so it does not promise a
  missing workflow.

The toolbar uses stable responsive dimensions and must fit the existing
530-pixel mobile viewport without text overlap.

## Testing

Backend tests cover:

- GEO grouping and startup initialization.
- Temporary-key replacement and removal of stale members.
- Distance ordering and pagination.
- Combined distance and area filtering.
- Popularity and score ordering with deterministic ties.
- Missing coordinates and invalid query parameters.
- GEO synchronization after shop creation and updates.
- Redis failure behavior.

Frontend and browser verification cover:

- The food category shows shops on first load.
- Type, area, distance, popularity, and score controls issue correct requests.
- Filter changes reset pagination and do not mix previous results.
- Infinite scrolling does not duplicate pages or concurrent requests.
- Empty and failed requests display useful states.
- The toolbar and shop content do not overlap at desktop and 530-pixel mobile
  viewports.

## Acceptance Criteria

- A clean Docker rebuild displays nearby food shops without manually running a
  test initializer.
- Redis contains `shop:geo:1` and `shop:geo:2` for the current seed data.
- Area, popularity, and score filters produce visible, correctly ordered lists.
- Existing category navigation and shop detail navigation continue to work.
- Automated backend tests pass under Java 8.
- Browser checks confirm the deployed Nginx-to-Docker flow, not only a local
  unit-test environment.
