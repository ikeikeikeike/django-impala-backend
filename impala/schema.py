from django.db.backends.schema import BaseDatabaseSchemaEditor
from django.db.models.fields.related import ManyToManyField


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):

    sql_delete_table = "DROP TABLE %(table)s"
    sql_create_column = "ALTER TABLE %(table)s ADD COLUMNS (%(column)s %(definition)s)"
    sql_delete_column = "ALTER TABLE %(table)s DROP COLUMN %(column)s"

    def quote_value(self, value):
        from impala import dbapi

        if value is None:
            return 'NULL'
        elif isinstance(value, basestring):
            return "'%s'" % dbapi._escape(value)
        else:
            return str(value)

    def column_sql(self, model, field, include_default=False):
        db_params = field.db_parameters(connection=self.connection)
        sql = db_params['type']
        if sql is None:
            return None, None
        return sql, []

    def create_model(self, model):
        params = []
        column_sqls = []

        for field in model._meta.local_fields:

            definition, extra_params = self.column_sql(model, field)
            if definition is None:
                continue
            col_type_suffix = field.db_type_suffix(connection=self.connection)
            if col_type_suffix:
                definition += " %s" % col_type_suffix
            params.extend(extra_params)

            column_sqls.append("%s %s" % (
                self.quote_name(field.column),
                definition,
            ))

            if field.get_internal_type() == "AutoField":
                autoinc_sql = self.connection.ops.autoinc_sql(model._meta.db_table, field.column)
                if autoinc_sql:
                    self.deferred_sql.extend(autoinc_sql)

        sql = self.sql_create_table % {
            "table": self.quote_name(model._meta.db_table),
            "definition": ", ".join(column_sqls)
        }

        self.execute(sql, params)

        for field in model._meta.local_many_to_many:
            if field.rel.through._meta.auto_created:
                self.create_model(field.rel.through)

    def alter_unique_together(self, *args, **kwargs):
        pass

    def alter_index_together(self, *args, **kwargs):
        pass

    def add_field(self, model, field):
        if isinstance(field, ManyToManyField) and field.rel.through._meta.auto_created:
            return self.create_model(field.rel.through)

        definition, params = self.column_sql(model, field, include_default=True)

        if definition is None:
            return

        sql = self.sql_create_column % {
            "table": self.quote_name(model._meta.db_table),
            "column": self.quote_name(field.column),
            "definition": definition,
        }
        self.execute(sql, params)

        if self.connection.features.connection_persists_old_columns:
            self.connection.close()
