"""The sqlite dialect.

https://www.sqlite.org/
"""

from typing import Optional

from sqlfluff.core.dialects import load_raw_dialect
from sqlfluff.core.parser import (
    AnyNumberOf,
    BaseSegment,
    Bracketed,
    Delimited,
    Indent,
    GreedyUntil,
    StartsWith,
    Nothing,
    Matchable,
    OneOf,
    OptionallyBracketed,
    Ref,
    Dedent,
    RegexLexer,
    Sequence,
    TypedParser,
)
from sqlfluff.dialects import dialect_ansi as ansi

ansi_dialect = load_raw_dialect("ansi")

sqlite_dialect = ansi_dialect.copy_as("sqlite")

sqlite_dialect.sets("reserved_keywords").update(["AUTOINCREMENT", "GLOB", "VIRTUAL", "MATERIALIZED"])
sqlite_dialect.sets("reserved_keywords").difference_update(["INTERVAL"])
sqlite_dialect.sets("unreserved_keywords").update(["FAIL", "INTERVAL"])

sqlite_dialect.replace(
    BooleanBinaryOperatorGrammar=OneOf(
        Ref("AndOperatorGrammar"), Ref("OrOperatorGrammar"), "REGEXP", "GLOB"
    ),
    PrimaryKeyGrammar=Sequence(
        "PRIMARY", "KEY", Sequence("AUTOINCREMENT", optional=True)
    ),
    GlobGrammar=OneOf("GLOB"),
)

sqlite_dialect.add(
    NamedWindowSegmentGrammar=OneOf(
        Sequence("ORDER", "BY"),
        "LIMIT",
    ),
)

class TableEndClauseSegment(BaseSegment):
    """Support WITHOUT ROWID at end of tables.

    https://www.sqlite.org/withoutrowid.html
    """

    type = "table_end_clause_segment"
    match_grammar: Matchable = Sequence("WITHOUT", "ROWID")


class IndexColumnDefinitionSegment(BaseSegment):
    """A column definition for CREATE INDEX.

    Overridden from ANSI to allow expressions
    https://www.sqlite.org/expridx.html.
    """

    type = "index_column_definition"
    match_grammar: Matchable = Sequence(
        OneOf(
            Ref("SingleIdentifierGrammar"),  # Column name
            Ref("ExpressionSegment"),  # Expression for simple functions
        ),
        OneOf("ASC", "DESC", optional=True),
    )


class InsertStatementSegment(BaseSegment):
    """An`INSERT` statement.

    https://www.sqlite.org/lang_insert.html
    """

    type = "insert_statement"
    match_grammar = Sequence(
        OneOf(
            Sequence(
                "INSERT",
                Sequence(
                    "OR",
                    OneOf(
                        "ABORT",
                        "FAIL",
                        "IGNORE",
                        "REPLACE",
                        "ROLLBACK",
                    ),
                    optional=True,
                ),
            ),
            # REPLACE is just an alias for INSERT OR REPLACE
            "REPLACE",
        ),
        "INTO",
        Ref("TableReferenceSegment"),
        Ref("BracketedColumnReferenceListGrammar", optional=True),
        OneOf(
            Ref("ValuesClauseSegment"),
            OptionallyBracketed(Ref("SelectableGrammar")),
            Ref("DefaultValuesGrammar"),
        ),
    )


class CTEDefinitionSegment(ansi.CTEDefinitionSegment):
    """A CTE Definition from a WITH statement.
    `tab (col1,col2) AS (SELECT a,b FROM x)`
    """

    type = "common_table_expression"
    match_grammar: Matchable = Sequence(
        Ref("SingleIdentifierGrammar"),
        Ref("CTEColumnList", optional=True),
        "AS",
        Ref.keyword("MATERIALIZED", optional=True),
        Bracketed(
            # Ephemeral here to subdivide the query.
            Ref("SelectableGrammar", ephemeral_name="SelectableGrammar")
        ),
    )


class ColumnConstraintSegment(ansi.ColumnConstraintSegment):
    """Overriding ColumnConstraintSegment to allow for additional segment parsing."""

    match_grammar = ansi.ColumnConstraintSegment.match_grammar.copy(
        insert=[
            OneOf("DEFERRABLE", Sequence("NOT", "DEFERRABLE"), optional=True),
            OneOf(
                Sequence("INITIALLY", "DEFERRED"),
                Sequence("INITIALLY", "IMMEDIATE"),
                optional=True,
            ),
        ],
    )


class TableConstraintSegment(ansi.TableConstraintSegment):
    """Overriding TableConstraintSegment to allow for additional segment parsing."""

    match_grammar: Matchable = Sequence(
        Sequence(  # [ CONSTRAINT <Constraint name> ]
            "CONSTRAINT", Ref("ObjectReferenceSegment"), optional=True
        ),
        OneOf(
            # CHECK ( <expr> )
            Sequence("CHECK", Bracketed(Ref("ExpressionSegment"))),
            Sequence(  # UNIQUE ( column_name [, ... ] )
                "UNIQUE",
                Ref("BracketedColumnReferenceListGrammar"),
                # Later add support for index_parameters?
            ),
            Sequence(  # PRIMARY KEY ( column_name [, ... ] ) index_parameters
                Ref("PrimaryKeyGrammar"),
                # Columns making up PRIMARY KEY constraint
                Ref("BracketedColumnReferenceListGrammar"),
                # Later add support for index_parameters?
            ),
            Sequence(  # FOREIGN KEY ( column_name [, ... ] )
                # REFERENCES reftable [ ( refcolumn [, ... ] ) ]
                Ref("ForeignKeyGrammar"),
                # Local columns making up FOREIGN KEY constraint
                Ref("BracketedColumnReferenceListGrammar"),
                Ref(
                    "ReferenceDefinitionGrammar"
                ),  # REFERENCES reftable [ ( refcolumn) ]
            ),
        ),
        OneOf("DEFERRABLE", Sequence("NOT", "DEFERRABLE"), optional=True),
        OneOf(
            Sequence("INITIALLY", "DEFERRED"),
            Sequence("INITIALLY", "IMMEDIATE"),
            optional=True,
        ),
    )


class VirtualTableModuleArgument(BaseSegment):
    """Foo"""

    type = "virtual_table_module_argument"
    match_grammar: Matchable = AnyNumberOf(
        OneOf(
            Ref("QuotedLiteralSegment"),
            Ref("NakedIdentifierSegment"),
            Ref("NumericLiteralSegment"),
        )
    )


class CreateVirtualTableSegment(BaseSegment):
    """A `CREATE VIRTUAL TABLE` statement.

    https://www.sqlite.org/lang_insert.html
    """

    type = "create_virtual_table_statement"
    match_grammar: Matchable = Sequence(
        "CREATE",
        "VIRTUAL",
        "TABLE",
        Ref("IfNotExistsGrammar", optional=True),
        Ref("TableReferenceSegment"),
        "USING",
        Ref("NakedIdentifierSegment"),
        Bracketed(
            Delimited(VirtualTableModuleArgument),
            optional=True,
        ),
    )


class StatementSegment(ansi.StatementSegment):
  """A generic segment, to any of its child subsegments."""
  type = "statement"
  match_grammar: Matchable = GreedyUntil(Ref("DelimiterGrammar"))

  parse_grammar: Matchable = OneOf(
        Ref("SelectableGrammar"),
        Ref("MergeStatementSegment"),
        Ref("InsertStatementSegment"),
        Ref("TransactionStatementSegment"),
        Ref("DropTableStatementSegment"),
        Ref("DropViewStatementSegment"),
        Ref("CreateUserStatementSegment"),
        Ref("DropUserStatementSegment"),
        Ref("TruncateStatementSegment"),
        Ref("AccessStatementSegment"),
        Ref("CreateTableStatementSegment"),
        Ref("CreateRoleStatementSegment"),
        Ref("DropRoleStatementSegment"),
        Ref("AlterTableStatementSegment"),
        Ref("CreateSchemaStatementSegment"),
        Ref("SetSchemaStatementSegment"),
        Ref("DropSchemaStatementSegment"),
        Ref("DropTypeStatementSegment"),
        Ref("CreateDatabaseStatementSegment"),
        Ref("DropDatabaseStatementSegment"),
        Ref("CreateIndexStatementSegment"),
        Ref("DropIndexStatementSegment"),
        Ref("CreateViewStatementSegment"),
        Ref("DeleteStatementSegment"),
        Ref("UpdateStatementSegment"),
        Ref("CreateFunctionStatementSegment"),
        Ref("DropFunctionStatementSegment"),
        Ref("CreateModelStatementSegment"),
        Ref("DropModelStatementSegment"),
        Ref("DescribeStatementSegment"),
        Ref("UseStatementSegment"),
        Ref("ExplainStatementSegment"),
        Ref("CreateSequenceStatementSegment"),
        Ref("AlterSequenceStatementSegment"),
        Ref("DropSequenceStatementSegment"),
        Ref("CreateTriggerStatementSegment"),
        Ref("DropTriggerStatementSegment"),
        Ref("CreateVirtualTableSegment"),
        Bracketed(Ref("StatementSegment")),
    )


class LimitClauseSegment(BaseSegment):
    """A `LIMIT` clause like in `SELECT`."""

    type = "limit_clause"
    match_grammar: Matchable = Sequence(
        "LIMIT",
        Indent,
        Ref("BaseExpressionElementGrammar"),
        Sequence(
            "OFFSET", Ref("BaseExpressionElementGrammar"),
            optional=True
        ),
        Dedent,
    )


class SelectStatementSegment(ansi.SelectStatementSegment):
    """A `SELECT` statement."""

    type = "select_statement"
    match_grammar: Matchable = ansi.SelectStatementSegment.match_grammar.copy()
    parse_grammar: Matchable = ansi.UnorderedSelectStatementSegment.parse_grammar.copy(
        insert=[
            Ref("NamedWindowSegment", optional=True),
            Ref("OrderByClauseSegment", optional=True),
            Ref("LimitClauseSegment", optional=True),
        ]
    )
