from django.utils import six
from django.db.backends import BaseDatabaseOperations


class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = __name__.rsplit('.', 1)[0] + '.compiler'

    def quote_name(self, name):
        return name

    def last_insert_id(self, cursor, table_name, pk_name):
        return '{last_insert_id}'

    def random_function_sql(self):
        return 'RAND()'

    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        # TODO: overwrite insert table by blank table.
        sql = ['%s %s;' % (
            style.SQL_KEYWORD('DROP TABLE'),
            style.SQL_FIELD(self.quote_name(table))
        ) for table in tables]

        return sql

    def value_to_db_datetime(self, value):
        if self.connection.features.supports_timezones:
            return super(DatabaseOperations, self).value_to_db_datetime(value)
        else:
            return six.text_type(value.replace(tzinfo=None))

    def start_transaction_sql(self):
        return ''

    def end_transaction_sql(self, success=True):
        return ''
