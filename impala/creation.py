import sys
import time
from django.utils.six.moves import input
from django.db.backends.creation import BaseDatabaseCreation


class DatabaseCreation(BaseDatabaseCreation):

    data_types = {
        'AutoField': 'bigint',  # TODO: string(uuid4)
        'BinaryField': 'string',
        'BigIntegerField': 'bigint',
        'BooleanField': 'boolean',
        'CharField': 'string',
        'CommaSeparatedIntegerField': 'string',
        'DateField': 'timestamp',
        'DateTimeField': 'timestamp',
        'DecimalField': 'decimal(%(max_digits)s, %(decimal_places)s)',
        'EmailField': 'string',
        'FileField': 'string',
        'FilePathField': 'string',
        'FloatField': 'float',
        'GenericIPAddressField': 'string',
        'ImageField': 'string',
        'IntegerField': 'bigint',
        'IPAddressField': 'string',
        'NullBooleanField': 'boolean',
        'OneToOneField': 'bigint',
        'PositiveIntegerField': 'bigint',
        'PositiveSmallIntegerField': 'int',
        'SlugField': 'string',
        'SmallIntegerField': 'smallint',
        'TextField': 'string',
        'TimeField': 'timestamp',
        'URLField': 'string',
        'XMLField': 'string',
    }

    data_type_check_constraints = {
        'PositiveIntegerField': '"%(column)s" >= 0',
        'PositiveSmallIntegerField': '"%(column)s" >= 0',
    }

    def sql_create_model(self, model, style, known_models=set()):
        opts = model._meta
        if not opts.managed or opts.proxy or opts.swapped:
            return [], {}

        final_output = []
        table_output = []
        pending_references = {}
        qn = self.connection.ops.quote_name

        for f in opts.local_fields:
            col_type = f.db_type(connection=self.connection)
            if col_type is None:
                continue

            field_output = [
                style.SQL_FIELD(qn(f.column)),
                style.SQL_COLTYPE(col_type)
            ]

            table_output.append(' '.join(field_output))

        full_statement = [
            style.SQL_KEYWORD('CREATE TABLE') + ' ' +
            style.SQL_TABLE(qn(opts.db_table)) + ' ('
        ]

        for i, line in enumerate(table_output):
            full_statement.append(
                '    %s%s' % (line, ',' if i < len(table_output) - 1 else ''))

        full_statement.append(')')

        # TODO: hive table space options

        full_statement.append(';')
        final_output.append('\n'.join(full_statement))

        return final_output, pending_references

    def _create_test_db(self, verbosity, autoclobber):
        qn = self.connection.ops.quote_name
        test_database_name = self._get_test_db_name()

        # Create the test database and connect to it.
        with self._nodb_connection.cursor() as cursor:
            try:
                cursor.execute("CREATE DATABASE %s" % qn(test_database_name))
            except Exception as e:
                sys.stderr.write(
                    "Got an error creating the test database: %s\n" % e)

                if not autoclobber:
                    confirm = input(
                        "Type 'yes' if you would like to try deleting the test "
                        "database '%s', or 'no' to cancel: " % test_database_name)

                if autoclobber or confirm == 'yes':
                    try:
                        if verbosity >= 1:
                            print("Destroying old test database '%s'..." %
                                  self.connection.alias)

                        self._destroy_test_db(test_database_name, verbosity)
                        cursor.execute("CREATE DATABASE %s" % qn(test_database_name))
                    except Exception as e:
                        sys.stderr.write(
                            "Got an error recreating the test database: %s\n" % e)
                        sys.exit(2)
                else:
                    print("Tests cancelled.")
                    sys.exit(1)

        return test_database_name

    def _destroy_test_db(self, test_database_name, verbosity):
        qn = self.connection.ops.quote_name

        with self._nodb_connection.cursor() as cursor:
            time.sleep(1)

            cursor.execute("use %s" % qn(test_database_name))
            cursor.execute('show tables')

            for name in cursor.fetchall():
                cursor.execute('DROP TABLE %s' % name[0])

            cursor.execute("use default")
            cursor.execute(
                "DROP DATABASE %s" % qn(test_database_name))

    def sql_indexes_for_model(self, model, style):
        return []

    def set_autocommit(self):
        pass
