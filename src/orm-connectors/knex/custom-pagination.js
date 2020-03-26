const base64 = require('base-64');
const apolloCursorPaginationBuilder = require('../../builder');
const { applyFilters } = require('knex-graphql-filters');

const SEPARATION_TOKEN = '_*_';
const ARRAY_DATA_SEPARATION_TOKEN = '_%_';

const encode = str => base64.encode(str);
const decode = str => base64.decode(str);

const operateOverScalarOrArray = (initialValue, scalarOrArray, operation, operateResult) => {
  let result = initialValue;
  const isArray = Array.isArray(scalarOrArray);
  if (isArray) {
    scalarOrArray.forEach((scalar, index) => {
      result = operation(scalar, index, result);
    });
  } else {
    result = operation(scalarOrArray, null, result);
  }
  if (operateResult) {
    result = operateResult(result, isArray);
  }

  return result;
};

const cursorGenerator = (id, customColumnValue) => encode(`${id}${SEPARATION_TOKEN}${customColumnValue}`);

const getDataFromCursor = (cursor) => {
  const decodedCursor = decode(cursor);
  const data = decodedCursor.split(SEPARATION_TOKEN);
  if (data[0] === undefined || data[1] === undefined) {
    throw new Error(`Could not find edge with cursor ${cursor}`);
  }
  const values = data[1].split(ARRAY_DATA_SEPARATION_TOKEN).map(v => JSON.parse(v));
  return [data[0], values];
};

const formatColumnIfAvailable = (column, formatColumnFn) => {
  if (formatColumnFn) {
    return formatColumnFn(column);
  }
  return column;
};

const buildRemoveNodesFromBeforeOrAfter = (beforeOrAfter) => {
  const getComparator = (orderDirection) => {
    if (beforeOrAfter === 'after') return orderDirection === 'asc' ? '<' : '>';
    return orderDirection === 'asc' ? '>' : '<';
  };
  const getGraphqlComparator = orderDirection => {
    if (beforeOrAfter === 'after') return orderDirection === 'asc' ? 'lt' : 'gt';
    return orderDirection === 'asc' ? 'gt' : 'lt';
  };
  return (nodesAccessor, cursorOfInitialNode, {
    idColumn = 'id',
    orderColumn, ascOrDesc, isAggregateFn, formatColumnFn,
  }) => {
    const data = getDataFromCursor(cursorOfInitialNode);
    const [id, columnValue] = data;

    const initialValue = nodesAccessor.clone();
    const executeFilterQuery = query => {
      const filters = operateOverScalarOrArray({ OR: []}, orderColumn, (orderBy, index, prev) => {
        let orderDirection;
        const values = columnValue;
        let currValue;
        if (index !== null) {
          orderDirection = ascOrDesc[index].toLowerCase();
          currValue = values[index];
        } else {
          orderDirection = ascOrDesc.toLowerCase();
          currValue = values[0];
        }
        const comparator = getComparator(orderDirection);
        const graphqlComparator = getGraphqlComparator(orderDirection);

        if (index > 0) {
          // TODO - sanity check the sort name
          const operation = (isAggregateFn && isAggregateFn(orderColumn[index - 1])) ? 'orHavingRaw' : 'orWhereRaw';

          const prevColumn = formatColumnIfAvailable(orderColumn[index - 1], formatColumnFn);
          const column = formatColumnIfAvailable(orderBy, formatColumnFn);
          prev.OR.push({
            AND: [{
              [prevColumn]: { is: values[index - 1] },
              [column]: { [graphqlComparator]: values[index] },
            }],
          });
          return prev;
        }

        if (currValue === null || currValue === undefined) {
          return prev;
        }

        const operation = (isAggregateFn && isAggregateFn(orderBy)) ? 'havingRaw' : 'whereRaw';
        const column = formatColumnIfAvailable(orderBy, formatColumnFn);

        prev.OR.push({
          [column]: { [graphqlComparator]: currValue },
        });
        return prev;
      }, (prev, isArray) => {
        // Result is sorted by id as the last column

        const comparator = getComparator(ascOrDesc);
        const graphqlComparator = getGraphqlComparator(ascOrDesc);
        const lastOrderColumn = isArray ? orderColumn.pop() : orderColumn;
        const lastValue = columnValue.pop();

        // If value is null, we are forced to filter by id instead
        const operation = (isAggregateFn && isAggregateFn(lastOrderColumn)) ? 'orHavingRaw' : 'orWhereRaw';
        if (lastValue === null || lastValue === undefined) {
          const idColumnName = formatColumnIfAvailable(idColumn, formatColumnFn);
          const lastOrderColumnName = formatColumnIfAvailable(lastOrderColumn, formatColumnFn);
          prev.OR.push({
            [idColumnName]: { [graphqlComparator]: id },
            [lastOrderColumnName]: { "not_null": true },
          });
          return prev;
        }

        const lastOrderColumnName = formatColumnIfAvailable(lastOrderColumn, formatColumnFn);
        const idColumnName = formatColumnIfAvailable(idColumn, formatColumnFn);

        prev.OR.push({
          AND: [
            {
              [lastOrderColumnName]: { "is": lastValue },
              [idColumnName]: { [graphqlComparator]: id },
            },
          ],
        });
        return prev;
      });

      console.log("filters=" + JSON.stringify(filters))
      return applyFilters(query, filters)
    }

    let result;
    result = executeFilterQuery(initialValue);

    /*
    if ((isAggregateFn && Array.isArray(orderColumn) && isAggregateFn(orderColumn[0]))
    || (isAggregateFn && !Array.isArray(orderColumn) && isAggregateFn(orderColumn))) {
      console.log("firstCall");
      result = executeFilterQuery(initialValue);
    } else {
      console.log("secondCall");
      result = initialValue.andWhere(query => executeFilterQuery(query));
    }
    */
    return result;
  };
};

const orderNodesBy = (nodesAccessor, { idColumn, orderColumn = 'id', ascOrDesc = 'asc', formatColumnFn }) => {
  const initialValue = nodesAccessor.clone();
  const result = operateOverScalarOrArray(initialValue, orderColumn, (orderBy, index, prev) => {
    if (index !== null) {
      return prev.orderBy(formatColumnIfAvailable(orderBy, formatColumnFn), ascOrDesc[index]);
    }
    return prev.orderBy(formatColumnIfAvailable(orderBy, formatColumnFn), ascOrDesc);
  }, (prev, isArray) => (isArray
    ? prev.orderBy(formatColumnIfAvailable(idColumn, formatColumnFn), ascOrDesc[0])
    : prev.orderBy(formatColumnIfAvailable(idColumn, formatColumnFn), ascOrDesc)));
  return result;
};

// Used when `after` is included in the query
// It must slice the result set from the element after the one with the given cursor until the end.
// e.g. let [A, B, C, D] be the `resultSet`
// removeNodesBeforeAndIncluding(resultSet, 'B') should return [C, D]
const removeNodesBeforeAndIncluding = buildRemoveNodesFromBeforeOrAfter('before');

// Used when `first` is included in the query
// It must remove nodes from the result set starting from the end until it's of size `length`.
// e.g. let [A, B, C, D] be the `resultSet`
// removeNodesFromEnd(resultSet, 3) should return [A, B, C]
const removeNodesFromEnd = (nodesAccessor, first) => nodesAccessor.clone().limit(first);

// Used when `before` is included in the query
// It must remove all nodes after and including the one with cursor `cursorOfInitialNode`
// e.g. let [A, B, C, D] be the `resultSet`
// removeNodesAfterAndIncluding(resultSet, 'C') should return [A, B]
const removeNodesAfterAndIncluding = buildRemoveNodesFromBeforeOrAfter('after');

// Used when `last` is included in the query
// It must remove nodes from the result set starting from the beginning until it's of size `length`.
// e.g. let [A, B, C, D] be the `resultSet`
// removeNodesFromBeginning(resultSet, 3) should return [B, C, D]
const removeNodesFromBeginning = (nodesAccessor, last, { orderColumn, ascOrDesc }) => {
  const invertedOrderArray = operateOverScalarOrArray([], ascOrDesc,
    (orderDirection, index, prev) => prev.concat(orderDirection === 'asc' ? 'desc' : 'asc'));

  const order = invertedOrderArray.length === 1 ? invertedOrderArray[0] : invertedOrderArray;

  // TODO - this seems like it might be broken.
  const subquery = orderNodesBy(nodesAccessor.clone().clearOrder(), orderColumn, order).limit(last);
  const result = nodesAccessor.clone().from(subquery.as('last_subquery')).clearSelect().clearWhere();
  return result;
};


const getNodesLength = async (nodesAccessor) => {
  const counts = await nodesAccessor.clone().count('*');
  const result = counts.reduce((prev, curr) => {
    const currCount = curr.count || curr['count(*)'];
    if (!currCount) return prev;
    return parseInt(currCount, 10) + prev;
  }, 0);
  return result;
};

const hasLengthGreaterThan = async (nodesAccessor, amount) => {
  const result = await nodesAccessor.clone().limit(amount + 1);
  return result.length === amount + 1;
};

// Receives a list of nodes and returns it in edge form:
// {
//   cursor
//   node
// }
const convertNodesToEdges = (nodes, _, {
  orderColumn,
}) => nodes.map((node) => {
  const dataValue = operateOverScalarOrArray('', orderColumn, (orderBy, index, prev) => {
    const nodeValue = node[orderBy];
    const result = `${prev}${index ? ARRAY_DATA_SEPARATION_TOKEN : ''}${JSON.stringify(nodeValue)}`;
    return result;
  });

  return {
    cursor: cursorGenerator(node.id, dataValue),
    node,
  };
});

const paginate = apolloCursorPaginationBuilder(
  {
    removeNodesBeforeAndIncluding,
    removeNodesAfterAndIncluding,
    getNodesLength,
    hasLengthGreaterThan,
    removeNodesFromEnd,
    removeNodesFromBeginning,
    convertNodesToEdges,
    orderNodesBy,
  },
);

module.exports = paginate;
module.exports.getDataFromCursor = getDataFromCursor;
module.exports.removeNodesBeforeAndIncluding = removeNodesBeforeAndIncluding;
module.exports.removeNodesFromEnd = removeNodesFromEnd;
module.exports.removeNodesAfterAndIncluding = removeNodesAfterAndIncluding;
module.exports.removeNodesFromBeginning = removeNodesFromBeginning;
module.exports.getNodesLength = getNodesLength;
module.exports.hasLengthGreaterThan = hasLengthGreaterThan;
module.exports.convertNodesToEdges = convertNodesToEdges;
