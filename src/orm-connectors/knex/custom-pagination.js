const base64 = require('base-64');
const apolloCursorPaginationBuilder = require('../../builder');
const { applyFilters } = require('knex-graphql-filters');

// We encode strings in the token using encodeURIComponent.  If the following tokens were
// in the values, they'd be encoded.
const SEPARATION_TOKEN = '/';

const encode = str => base64.encode(str);
const decode = str => base64.decode(str);

// TODO - this can probably be removed to make the code easier to read.
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
  return decodedCursor.split(SEPARATION_TOKEN).map(v => JSON.parse(decodeURIComponent(v)));
};

// Receives a list of nodes and returns it in edge form:
// {
//   cursor
//   node
// }
const convertNodesToEdges = (nodes, _, {
  idColumn,
  orderColumn,
}) => nodes.map((node) => {
  orderColumn = combineOrderColumn(orderColumn, idColumn);
  const cursor = operateOverScalarOrArray('', orderColumn, (orderBy, index, prev) => {
    const nodeValue = node[orderBy];
    const result = `${prev}${index ? SEPARATION_TOKEN : ''}${encodeURIComponent(JSON.stringify(nodeValue))}`;
    return result;
  });

  return {
    cursor,
    node,
  };
});

const formatColumnIfAvailable = (column, formatColumnFn) => {
  if (formatColumnFn) {
    return formatColumnFn(column);
  }
  return column;
};

// Returns true if we find findColumn in orderColumn.
const idContains = (orderColumn, findSingleColumn) => {
  if (Array.isArray(orderColumn)) {
    return orderColumn.includes(findSingleColumn);
  } else {
    return orderColumn === findSingleColumn;
  }
}

const combineOrderColumn = (orderColumn, idColumn) => {
  var result;
  if (Array.isArray(orderColumn)) {
    result = [...orderColumn];
  } else {
    result = [orderColumn];
  }
  if (Array.isArray(idColumn)) {
    if (!idContains(orderColumn, idColumn)) {
      result.push(idColumn);
    }
  } else {
    idColumn.forEach((idCol) => {
      if (!idContains(orderColumn, idCol)) {
        result.push(idCol);
      }
    });
  }
  return results;
}

// Do a combined version since we de-dupe for both results.
// This returns a tuple array with both orderColumn and ascOrDesc.
const combineOrderColumnAndAscOrDesc = (orderColumn, idColumn, ascOrDesc) => {
  var resultOrderColumns;
  var resultAscOrDesc;
  if (Array.isArray(orderColumn)) {
    resultOrderColumns = [...orderColumn];
    resultAscOrDesc = [...ascOrDesc];
  } else {
    resultOrderColumns = [orderColumn];
    resultAscOrDesc = [ascOrDesc];
  }
  const lastAscOrDesc = resultAscOrDesc[resultAscOrDesc.length - 1];
  if (Array.isArray(idColumn)) {
    idColumn.forEach((idCol) => {
      if (!idContains(orderColumn, idCol)) {
        resultOrderColumns.push(idCol);
        resultAscOrDesc.push(lastAscOrDesc);
      }
    });
  } else {
    if (!idContains(orderColumn, idColumn)) {
      resultOrderColumns.push(idColumn);
      resultAscOrDesc.push(lastAscOrDesc);
    }
  }
  return [resultOrderColumns, resultAscOrDesc];
}


const buildRemoveNodesFromBeforeOrAfter = (beforeOrAfter) => {
  const getComparator = orderDirection => {
    if (beforeOrAfter === 'after') return orderDirection === 'asc' ? 'lt' : 'gt';
    return orderDirection === 'asc' ? 'gt' : 'lt';
  };
  return (nodesAccessor, cursorOfInitialNode, {
    idColumn = 'id',
    orderColumn, ascOrDesc, isAggregateFn, formatColumnFn,
  }) => {
    const cursorValues = getDataFromCursor(cursorOfInitialNode);

    const executeFilterQuery = query => {

      // The following doc talks about how the origin branch has an incorrect multiple column implementation.
      // https://docs.google.com/document/d/1G_7JCNgkg4avwZCojwdwSmJoFvvbr5_K4UJpRXlTq-k/edit#

      // Merge idColumn into a copy of orderColumn and ascOrDesc.
      [orderColumn, ascOrDesc] = combineOrderColumnAndAscOrDesc(orderColumn, idColumn, ascOrDesc);

      const filters = operateOverScalarOrArray({ OR: []}, orderColumn, (orderBy, index, prev) => {
        let orderDirection;
        let currValue;
        if (index !== null) {
          orderDirection = ascOrDesc[index].toLowerCase();
          currValue = cursorValues[index];
        } else {
          orderDirection = ascOrDesc.toLowerCase();
          currValue = cursorValues[0];
        }

        // TODO - handle having vs where.

        const conditions = {};
        // For each pass, we want the previous columns to match their value.
        // TODO - add description for why.
        for (var i = 0; i < index; i++) {
          const columnName = formatColumnIfAvailable(orderColumn[i], formatColumnFn);
          conditions[columnName] = { is: cursorValues[i] };
        }
        // The condition for the current index should use the comparator.
        const columnName = formatColumnIfAvailable(orderBy, formatColumnFn);

        // We treat NULL as 0.  If we're trying to reach values that are
        // not possible < NULL or > NOT NULL, skip that condition (since it's an OR).
        if (currValue === null || currValue === undefined) {
          if (orderDirection === 'desc') {
            // With our current sorting, We can't a value less than null.
            return prev;
          } else {
            // We consider "not null" to be greater than "null".
            conditions[columnName] = { not_null: true };
          }
        } else {
          const comparator = getComparator(orderDirection);
          conditions[columnName] = { [comparator]: currValue };
        }

        if (index == 0) {
          // We don't need wrapping parantheses.
          prev.OR.push(conditions);
        } else {
          prev.OR.push({
            AND: [conditions]
          });
        }
        return prev;
      });

      const areAnyColumnsAggregates =
        orderColumn.reduce((accumulator, currentColumn) => accumulator || isAggregateFn(currentColumn), false);
      const opts = {};
      if (areAnyColumnsAggregates) {
        opts.having = true;
      }
      return applyFilters(query, filters, opts);
    }

    return executeFilterQuery(nodesAccessor.clone());
  };
};

const orderNodesBy = (nodesAccessor, { idColumn, orderColumn = 'id', ascOrDesc = 'asc', formatColumnFn }) => {
  [orderColumn, ascOrDesc] = combineOrderColumnAndAscOrDesc(orderColumn, idColumn, ascOrDesc);

  const initialValue = nodesAccessor.clone();
  const result = operateOverScalarOrArray(initialValue, orderColumn, (orderBy, index, prev) => {
    if (index !== null) {
      return prev.orderBy(formatColumnIfAvailable(orderBy, formatColumnFn), ascOrDesc[index]);
    }
    return prev.orderBy(formatColumnIfAvailable(orderBy, formatColumnFn), ascOrDesc);
  });
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
const removeNodesFromBeginning = async (nodesAccessor, last, { orderColumn, ascOrDesc }) => {
  // When doing a 'before', the underly query sorts get flipped.  We don't want the results to be flipped though when the results are returned.
  // E.g. think about how this will be used.  Someone is paging through results and wants to go to the previous page.  They want the overall
  // sort to be the same regardless of their direction.
  //
  // So what we'll do is we'll use the 'before's flipped results and flip them back on the client side.
  const nodes = await nodesAccessor.clone().limit(last);
  return [...nodes].reverse();
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
