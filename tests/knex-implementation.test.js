const base64 = require('base-64');
const knexCreator = require('knex');
const knexStringcase = require('knex-stringcase');
const converterFactory = require('knex-stringcase/converter-factory');
const { knexPaginator } = require('../src');
const { applyFilters } = require('knex-graphql-filters');

const cachedSnakecase = converterFactory('snakecase');

// We want to assert against the query strings.  The pagination code is structured to
// execute the queries.  To work around this, we'll wrap the knex query and capture
// strings and return dummy data.
class OutputQueryStringsKnex {
  constructor(query, queryStringsOutput) {
    this.query = query;
    this.queryStringsOutput = queryStringsOutput;
  }

  clone() {
    return new OutputQueryStringsKnex(this.query.clone(), this.queryStringsOutput);
  }

  orderBy(...args) {
    this.query.orderBy(...args);
    return this;
  }

  andWhere(...args) {
    this.query.andWhere(...args);
    return this;
  }

  orWhere(...args) {
    this.query.orWhere(...args);
    return this;
  }

  count(...args) {
    this.query.count(...args);
    return this;
  }

  limit(...args) {
    this.query.limit(...args);
    return this;
  }

  then(resolveFn, rejectFn) {
    console.log("this.query=" + this.query);
    this.queryStringsOutput.push(this.query.toString());
    resolveFn([]);
  }
}

describe('test where clause', () => {
  function paginate(baseQuery, cursorInput, idName) {
    return knexPaginator(baseQuery, cursorInput, {
      idColumn: idName,
      isAggregateFn: (column) => {
        return column == "metric";
      },
      formatColumnFn: column => {
        if (column == "metric") {
          return "sum(metric)";
        } else if (column == "fourthColumn") {
          return "custom_forth_column";
        }
        return cachedSnakecase(column);
      },
    });
  }

  function createTestKnex() {
    const knex = knexCreator(
      knexStringcase({
        // The mysql client causes weird backticks that breaks Presto.
        client: 'postgres',
      })
    );
    return knex("mytable").select("metric");
  }

  function wrapKnex(query, queryStringsOutput) {
    return new OutputQueryStringsKnex(query, queryStringsOutput);
  };

  const createTestCursorInput = () => ({
    first: 3,
    after: base64.encode("1_*_2"),
    orderBy: "firstId",
    orderDirection: "desc"
  });

  it('no sort', async () => {
    const queryStrings = [];
    const cursorInput = {};
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(1);
    expect(queryStrings[0]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('has sort desc', async () => {
    const queryStrings = [];
    const cursorInput = createTestCursorInput();
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < 2 or (\"first_id\" = 2 and \"backup_id\" < '1')) order by \"first_id\" desc, \"backup_id\" desc limit 4");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('has sort asc', async () => {
    const queryStrings = [];
    const cursorInput = createTestCursorInput();
    cursorInput.orderDirection = "asc";
    await paginate(wrapKnex(createTestKnex(), queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" > 2 or (\"first_id\" = 2 and \"backup_id\" > '1')) order by \"first_id\" asc, \"backup_id\" asc limit 4");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('already has filters', async () => {
    const queryStrings = [];
    const cursorInput = createTestCursorInput();
    const query = createTestKnex();
    query.where("otherId", "abc")
    await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" where \"other_id\" = 'abc' and (\"first_id\" < 2 or (\"first_id\" = 2 and \"backup_id\" < '1')) order by \"first_id\" desc, \"backup_id\" desc limit 4");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\" where \"other_id\" = 'abc'");
  });

  it('multiple sorts - 2 columns', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 3,
      after: base64.encode("1_*_2_%_3"),
      orderBy: ["firstId", "secondId"],
      orderDirection: ["desc", "asc"],
    };
    const query = createTestKnex();
    await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < 2 or (\"first_id\" = 2 and \"second_id\" > 3) or (\"second_id\" = 3 and \"backup_id\" < '1')) order by \"first_id\" desc, \"second_id\" asc, \"backup_id\" desc limit 4");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  it('multiple sorts - 3 columns', async () => {
    const queryStrings = [];
    const cursorInput = {
      first: 3,
      after: base64.encode("1_*_2_%_3_%_4"),
      orderBy: ["firstId", "secondId", "thirdId"],
      orderDirection: ["desc", "asc", "desc"],
    };
    const query = createTestKnex();
    await paginate(wrapKnex(query, queryStrings), cursorInput, 'backupId');
    expect(queryStrings.length).toEqual(2);
    expect(queryStrings[0])
      .toEqual("select \"metric\" from \"mytable\" where (\"first_id\" < 2 or (\"first_id\" = 2 and \"second_id\" > 3) or (\"second_id\" = 3 and \"third_id\" < 4) or (\"third_id\" = 4 and \"backup_id\" < '1')) order by \"first_id\" desc, \"second_id\" asc, \"third_id\" desc, \"backup_id\" desc limit 4");
    expect(queryStrings[1]).toEqual("select \"metric\", count(*) from \"mytable\"");
  });

  // TODO - null value.
  // TODO - having columns.
  // TODO - formatColumn.

  // TODO - look at other todo.
  // TODO - which way to sort id column?


});
