using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text;

namespace Tools
{
    public class Connection
    {
        private readonly string _connectionString;
        private readonly DbProviderFactory _factory;
        public Connection(DbProviderFactory factory, string connectionString)
        {
            _factory = factory;
            _connectionString = connectionString;
            using (DbConnection connection = GetConnection())
            {
                connection.Open();
            }
        }
        public int ExecuteNonQuery(Command command)
        {
            using (DbConnection connection = GetConnection())
            {
                using (DbCommand SqlCommand = CreateCommand(connection,command))
                {
                    connection.Open();
                    return SqlCommand.ExecuteNonQuery();
                }
            }
        }
        public object ExecuteScalar(Command command)
        {
            using(DbConnection connection = GetConnection())
            {
                using(DbCommand SqlCommand = CreateCommand(connection,command))
                {
                    connection.Open();
                    return SqlCommand.ExecuteScalar();
                }
            }
        }
        public IEnumerable<TResult> ExecuteReader<TResult>(Command command,Func<IDataRecord,TResult> selector)
        {
            using(DbConnection connection = GetConnection())
            {
                using(DbCommand sqlCommand = CreateCommand(connection, command))
                {
                    connection.Open();
                    using (IDataReader dataReader = sqlCommand.ExecuteReader())
                    {
                        while(dataReader.Read())
                        {
                            yield return selector(dataReader);
                        }
                    }
                }
            }
        }
        public DataTable GetDataTable(Command command)
        {
            using (DbConnection connection = GetConnection())
            {
                using (DbCommand SqlCommand = CreateCommand(connection, command))
                {
                    connection.Open();
                    using (DbDataAdapter dataAptater = _factory.CreateDataAdapter())
                    {
                        dataAptater.SelectCommand = SqlCommand;
                        DataTable dataTable = new DataTable();
                        dataAptater.Fill(dataTable);
                        return dataTable;
                    }
                }
            }
        }

        private DbConnection GetConnection()
        {
            SqlConnection connection = new SqlConnection();
            connection.ConnectionString = _connectionString;
            return connection;
        }

        private DbCommand CreateCommand(DbConnection connection, Command command)
        {
            DbCommand sqlCommand = connection.CreateCommand();
            sqlCommand.CommandText = command.Query;

            if(command.IsStoredProcedure)
            {
                sqlCommand.CommandType = CommandType.StoredProcedure;
            }
            foreach(KeyValuePair<string,object> kvp in command.Parameters)
            {
                DbParameter sqlParameter = _factory.CreateParameter();
                sqlParameter.ParameterName = kvp.Key;
                sqlParameter.Value = kvp.Value;

                sqlCommand.Parameters.Add(sqlParameter);
            }
            return sqlCommand;
        }
    }
}
