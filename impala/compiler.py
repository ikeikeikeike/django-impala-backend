import uuid
from django.db.models.sql import compiler


class SQLCompiler(compiler.SQLCompiler):
    pass


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):

    _latest_param = None

    def as_sql(self):
        opts = self.query.get_meta()
        origfields = self.query.fields

        if opts.has_auto_field:
            self.query.fields = [opts.pk] + self.query.fields

        queries = []
        for sql, params in super(SQLInsertCompiler, self).as_sql():
            if opts.has_auto_field:
                params[0] = int(str(int(uuid.uuid4()))[:18])  # TODO: string(uuid4)

            self._latest_param = params
            queries.append((sql, params))

        self.query.fields = origfields
        return queries

    def execute_sql(self, return_id=False):
        result = (
            super(SQLInsertCompiler, self)
            .execute_sql(return_id=return_id))

        if not result:
            return

        return result.format(
            last_insert_id=self._latest_param[0])


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass


class SQLDateCompiler(compiler.SQLDateCompiler, SQLCompiler):
    pass


class SQLDateTimeCompiler(compiler.SQLDateTimeCompiler, SQLCompiler):
    pass
