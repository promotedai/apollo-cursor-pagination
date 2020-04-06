# IMPORTANT!

This code has been forked and changed to actually work for cases that promoted.ai needs.

Here are some of the bugs in the original implementation:

1. The cursor sorting logic was [wrong for more than two columns](https://docs.google.com/document/d/1G_7JCNgkg4avwZCojwdwSmJoFvvbr5_K4UJpRXlTq-k/edit?usp=drive_web&ouid=117239387681127446188).

2. Having clauses did not work correctly.  This made it impossible to use it for stats sorting.

3. Backup id column sorting could not be customized and did not support multiple id columns.

4. The cursor will break if '_*_' or '_%_' are in the string values.


Dev notes:

1. The main adsmgr-api depends on src/.  dist/ and the build commands are not used.  I'm leaving them in case there are other issues.

# Testing

The original tests do not run.  I added a special test for the main file I changed (related to building SQL).

Run `yarn test tests/knex-implementation.test.js --watch`.

You can run the adsmgr-api tests using a local version of the library by doing the following:

1. Go to `apollo-cursor-pagination` directory and run `npm link`.

2. Go to `adsmgr-api` directory and run `npm link apollo-cursor-pagination`.

3. Run tests.

4. To reverse, call `npm unlink` and `npm unlink apollo-cursor-pagination` in the appropriate directory.

# Here are the original README notes.

# Apollo Cursor Pagination

Implementation of Relay's Connection specs for Apollo Server. Allows your Apollo Server to do cursor-based pagination. It can connect to any ORM, but only the connection with Knex.js is implemented currently.

## Installation

`yarn add apollo-cursor-pagination`

## Usage

### Using an existing connector

Use the `paginate` function of the connector in your GraphQL resolver.

`paginate` receives the following arguments:

1- `nodesAccessor`: An object that can be queried or accessed to obtain a reduced result set.

2- `args`: GraphQL args for your connection. Can have the following fields: `first`, `last`, `before`, `after`.

3- `orderArgs`: If using a connector with stable cursor, you must indicate to `paginate` how are you sorting your query. Must contain `orderColumn` (which attribute you are ordering by) and `ascOrDesc` (which can be `asc` or `desc`). Note: apollo-cursor-pagination does not sort your query, you must do it yourself before calling `paginate`.

For example with the knex connector:

```javascript
// cats-connection.js
import { knexPaginator as paginate } from 'apollo-cursor-pagination';
import knex from '../../../db'; // Or instantiate a connection here

export default async (_, args) => {
  // orderBy must be the column to sort with or an array of columns for ordering by multiple fields
  // orderDirection must be 'asc' or 'desc', or an array of those values if ordering by multiples
  const {
    first, last, before, after, orderBy, orderDirection,
  } = args;

  const baseQuery = knex('cats');

  const result = await paginate(baseQuery, {first, last, before, after, orderBy, orderDirection});
  /* result will contain:
  * edges
  * totalCount
  * pageInfo { hasPreviousPage, hasNextPage, }
  */
  return result;
};
```

#### Formatting Column Names

If you are using something like [Objection](https://vincit.github.io/objection.js/) and have
mapped the column names to something like snakeCase instead of camel_case, you'll want to use
the `formatColumnFn` option to make sure you're ordering by and building cursors from the correct
column name:

```javascript
const result = await paginate(
  baseQuery,
  { first, last, before, after, orderBy, orderDirection },
  {
    formatColumnFn: (column) => {
      // Logic to transform your column name goes here...
      return column
    }
  }
);
```

**Important note:** Make sure you pass the un-formatted version as your `orderBy` argument. It helps
to create a wrapper around the `paginate()` function that enforces this. For example, if the formatted
database column name is "created_at" and the column name on the model is "createdAt," you would pass
"createdAt" as the `orderBy` argument.

<details>
  <summary>An example with Objection columnNameMappers</summary>

  ```javascript
  const result = await paginate(
    baseQuery,
    { first, last, before, after, orderBy, orderDirection },
    {
      formatColumnFn: (column) => {
        if (Model.columnNameMappers && Model.columnNameMappers.format) {
          const result = Model.columnNameMappers.format({ [column]: true })
          return Object.keys(result)[0]
        } else {
          return column
        }
      }
    }
  );
  ```
</details>

#### Customizing Edges

If you have additional metadata you would like to pass along with each edge, as is allowed by the Relay
specification, you may do so using the `modifyEdgeFn` option:

```javascript
const result = await paginate(
  baseQuery,
  { first, last, before, after, orderBy, orderDirection },
  {
    modifyEdgeFn: (edge) => ({
      ...edge,
      custom: 'foo',
    })
  }
);
```

### Creating your own connector

Only Knex.js is implemented for now. If you want to connect to a different ORM, you must make your own connector.

To create your own connector:

1- Import `apolloCursorPaginationBuilder` from `src/builder/index.js`

2- Call `apolloCursorPaginationBuilder` with the specified params. It will generate a `paginate` function that you can export to use in your resolvers.

You can base off from `src/orm-connectors/knex/custom-pagination.js`.

## Contributing

Pull requests are welcome, specially to implement new connectors for different ORMs / Query builders.

When submitting a pull request, please include tests for the code you are submitting, and check that you did not break any working test.

### Testing and publishing changes

1- Code your changes

2- Run `yarn build`. This will generate a `dist` folder with the distributable files

3- `cd` into the test app and run `yarn install` and then `yarn test`. Check that all tests pass.

4- Send the PR. When accepted, the maintainer will publish a new version to npm using the new `dist` folder.

### Running the test suite

1- `cd tests/test-app`

2- `yarn install`

3- `yarn test`
