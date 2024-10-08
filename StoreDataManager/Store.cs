﻿using Entities;
using System.Data;
using System.Text;
using StoreDataManager.StoreOperations;

namespace StoreDataManager
{
    public sealed class Store
    {
        private static Store? instance = null;
        private static readonly object _lock = new object();

        public static Store GetInstance()
        {
            lock(_lock)
            {
                if (instance == null) 
                {
                    instance = new Store();
                }
                return instance;
            }
        }

        private const string DatabaseBasePath = @"C:\TinySql\"; //Se crea la carpeta que contendrá todo lo relacionado a la data del programa.
        private const string DataPath = $@"{DatabaseBasePath}\Data"; //Además se crea una carpeta que contendrá la data, es decir las bases de datos y demás.
        //Pasó a ser global en el archivo de configuración de rutas "ConfigPaths" -- > private const string SystemCatalogPath = $@"{DataPath}\SystemCatalog";//Contendrá archivos binarios con la información total de todas las bases de datos, con sus tablas y demás.
        private string RutaDeterminadaPorSet = $@"{DataPath}\"; //Ruta que cambiará constantemente ya que la determina la operación SET
        private int? currentDatabaseId; // Variable para almacenar el ID de la base de datos seleccionada

        public Store()
        {
            this.InitializeSystemCatalog();
        }

        private void InitializeSystemCatalog() {
            // Preparamos los archivos a crear dentro de SystemCatalog
            string[] catalogFiles = {
                Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemDatabases.databases"), //Almacena ID y nombre de DB
                Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemTables.tables"), //Almacena ID de Db, ID de tabla y nombre de Tabla
                Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemColumns.columns"), //Almacena ID Table, Nombre columna, Tipo de dato, Indicadores de si es nullable, primary key, y la longitud en caso de ser un VARCHAR
                Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemIndexes.Indexes") // x
            };

            // Crear cada archivo si no existe
            foreach (string filePath in catalogFiles) {
                if (!File.Exists(filePath)) {
                    using (var stream = File.Create(filePath)) {
                        // Archivo creado vacío
                    }
                }
            }
        }

        //-----------------------------Apartir de aquí son las operaciones-------------------------------------------
        public OperationStatus CreateDataBase(string databaseName)
        {
            var createDatabaseOperation = new CreateDatabaseOperation(DataPath, Entities.ConfigPaths.SystemCatalogPath);
            return createDatabaseOperation.Execute(databaseName);
        }

        public OperationStatus Set(string DataBaseToSet) 
        { //Cambia la ruta donde crear tablas , es decir, en que base de datos crear las tablas.
            var setOperation = new SetOperation(DataPath, Entities.ConfigPaths.SystemCatalogPath);
            var (status, message, newPath, newDatabaseId) = setOperation.Execute(DataBaseToSet);

            if (status == OperationStatus.Success)
            {
                RutaDeterminadaPorSet = newPath;
                currentDatabaseId = newDatabaseId;
                Console.WriteLine(message);
            }
            else
            {
                Console.WriteLine(message);
            }

            return status;
        }

        public OperationStatus CreateTable(string tableName, List<ColumnDefinition> columns) //Operación para poder crear tablas vacías pero con encabezados a los cuales agregarles datos.
        {
            var createTableOperation = new CreateTableOperation(DataPath, Entities.ConfigPaths.SystemCatalogPath, currentDatabaseId);
            return createTableOperation.Execute(tableName, columns, RutaDeterminadaPorSet);
        }

        public OperationStatus InsertIntoTable(string tableName, string[] columnas, string[] valores) //Permite insertar los datos en alguna tabla
        {//pero solo si se verificaron que dichos datos cumplen con la estructura esperada, esto se logra comparar en la clase dedicada para la operación
            // Insert.cs en Operations en QueryProcessor.

            var insertOperation = new InsertIntoTableOperation(DataPath, Entities.ConfigPaths.SystemCatalogPath);
            return insertOperation.Execute(tableName, columnas, valores, RutaDeterminadaPorSet);
        }

        public OperationStatus DropTable(string tableName)
        {
            var dropTableOperation = new DropTableOperation(DataPath, Entities.ConfigPaths.SystemCatalogPath);
            return dropTableOperation.Execute(tableName, RutaDeterminadaPorSet);
        }

                //!!!!!!!!!!!!!!!!!!!!Este método tiene que ser reestructurado según como se pide en el documento.!!!!!!!!!!!!!!!!!!!!!
        public (OperationStatus Status, string Data) Select(string tableName, string[] columnsToSelect, string whereClause = null) //Permite leer todo el contenido de un archivo binario(Tablas)
        {
            //TODO: Aplicar el whereClause y seleccionar solo las columnas que se pidan
            // Prepara el nombre completo del archivo de la tabla
            string nombreTabla = tableName + ".Table"; //Se preapara la tabla a leer.
            string fullPath = Path.Combine(RutaDeterminadaPorSet, tableName);//Se combina toda la ruta

            // Log para depuración
            Console.WriteLine($"Attempting to select from table: {tableName}");
            Console.WriteLine($"Full path: {fullPath}");

            // Verifica si el archivo de la tabla existe.
            if (!File.Exists(fullPath))//Prevención de errores, la tabla no existe.
            {
                Console.WriteLine($"Error: The table file '{fullPath}' does not exist.");
                return (OperationStatus.Error, $"Error: La tabla '{tableName}' no existe.");
            }
        
            StringBuilder resultBuilder = new StringBuilder(); //Creamos una string mutable que pueda albergar toda la estructura que contiene el archivo binario.
            try
            {
                using (FileStream stream = File.Open(fullPath, FileMode.Open))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    // Verificar la marca de inicio, por eso era tan importante añadirlo en la InsertOperation.
                    string startMarker = reader.ReadString();
                    if (startMarker != "TINYSQLSTART")
                    {
                        throw new InvalidDataException("Formato de archivo inválido.");
                    }
        
                    // Leer la estructura de la tabla
                    int columnCount = reader.ReadInt32();
                    List<ColumnDefinition> columns = new List<ColumnDefinition>();

                    List<ColumnDefinition> selectedColumns;
                    if (columnsToSelect.Length == 1 && columnsToSelect[0] == "*")
                    {
                        selectedColumns = columns; // Selecciona todas las columnas
                    }
                    else
                    {
                        selectedColumns = columns.Where(c => columnsToSelect.Contains(c.Name)).ToList();
                    }

                    //TODO: Implementar la selección de columnas y la cláusula WHERE
                    /*
                    
                    while (stream.Position < stream.Length)
                    {
                        StringBuilder ConstructorFila = new StringBuilder();
                        bool matchesWhereClause = true;

                        foreach (var column in allColumns)
                        {
                            string value = null;

                            // Leer los valores según el tipo de dato
                            switch (column.DataType)
                            {
                                case "INTEGER":
                                    value = reader.ReadInt32().ToString();
                                    break;
                                case "DOUBLE":
                                    value = reader.ReadDouble().ToString();
                                    break;
                                case "DATETIME":
                                    long ticks = reader.ReadInt64();
                                    DateTime dateTime = new DateTime(ticks);
                                    value = dateTime.ToString("yyyy-MM-dd HH:mm:ss");
                                    break;
                                default:
                                    int length = reader.ReadInt32();
                                    value = new string(reader.ReadChars(length));
                                    break;
                            }

                            // Solo agrega las columnas seleccionadas
                            if (selectedColumns.Any(c => c.Name == column.Name))
                            {
                                ConstructorFila.Append(value + ",");
                            }

                            // Verificar la cláusula WHERE si aplica
                            if (!string.IsNullOrEmpty(whereClause))
                            {
                                var whereParts = whereClause.Split(' ');
                                string whereColumn = whereParts[0]; // Nombre de la columna
                                string operatorSymbol = whereParts[1]; // Operador (=, >, <, etc.)
                                string whereValue = whereParts[2]; // Valor de comparación

                                if (column.Name == whereColumn)
                                {
                                    matchesWhereClause = EvaluateWhereClause(value, operatorSymbol, whereValue);
                                }
                            }
                        }

                        if (matchesWhereClause)
                        {
                            resultBuilder.AppendLine(ConstructorFila.ToString().TrimEnd(','));
                        }
                    }
                    
                    Creado desde GPT

                    
                    */
                    for (int i = 0; i < columnCount; i++) //Se comienza a añadir como columnas los encabezados
                    {
                        
                        var column = new ColumnDefinition//<-- Este archivo se encuentra en Entities, permite definir que tipos de datos se esperan y cuales son.
                        {
                            Name = reader.ReadString(),
                            DataType = reader.ReadString(),
                            IsNullable = reader.ReadBoolean(),
                            IsPrimaryKey = reader.ReadBoolean(),
                            VarcharLength = reader.ReadInt32()
                        };
                        Console.WriteLine($"Columna agregada: Name={column.Name}, Type={column.DataType}, Nullable={column.IsNullable}, PrimaryKey={column.IsPrimaryKey}, VarcharLength={column.VarcharLength}");
                        columns.Add(column);
                    }
        
                    // Verificar la marca de fin de estructura
                    string endStructureMarker = reader.ReadString();
                    if (endStructureMarker != "ENDSTRUCTURE")
                    {
                        throw new InvalidDataException("Invalid file structure");
                    }
        
                    // Construye el encabezado del resultado
                    resultBuilder.AppendLine(string.Join(",", columns.Select(c => c.Name)));

                    //-----------------------Apartir de aquí comienza a añadir los datos almacenados------------------------------------------------------
                    // Buscar el inicio de los datos
                    string dataStartMarker = reader.ReadString();
                    if (dataStartMarker != "DATASTART")
                    {
                        throw new InvalidDataException("Marca donde comienza la información no encontrada");
                    }

                    //Depuración
                    Console.WriteLine($"Longitud del archivo: {stream.Length}, Posición actual: {stream.Position}");
        
                    bool hasData = false; //<--- Se usa para poder devolver un mensaje indicando si se encontró o no información en la tabla actual.
                    // Lee los datos de cada fila
                    while (stream.Position < stream.Length)
                    {
                        hasData = true;
                        StringBuilder ConstructorFila = new StringBuilder(); //<--- Encargado de guardar todos los datos de las filas en las tablas.
                        foreach (var column in columns)
                        {
                            // Leer el valor según el tipo de dato, lo vamos agregando 
                            switch (column.DataType)
                            {
                                case "INTEGER":
                                    ConstructorFila.Append(reader.ReadInt32());
                                    break;
                                case "DOUBLE":
                                    ConstructorFila.Append(reader.ReadDouble());
                                    break;
                                case "DATETIME":
                                    long ticks = reader.ReadInt64();
                                    DateTime dateTime = new DateTime(ticks);
                                    ConstructorFila.Append(dateTime.ToString("yyyy-MM-dd HH:mm:ss"));
                                    break;
                                default: // VARCHAR para los Nombres y Apellidos en caso de...
                                    int length = reader.ReadInt32();
                                    ConstructorFila.Append(new string(reader.ReadChars(length)));
                                    break;
                            }
                            ConstructorFila.Append(",");
                        }
                        resultBuilder.AppendLine(ConstructorFila.ToString().TrimEnd(','));
                    }

                    // Verifica si se encontraron datos
                    if (!hasData)
                    {
                        Console.WriteLine("La tabla está vacía.");
                        return (OperationStatus.Success, "La tabla está vacía.");
                    }
        
                    Console.WriteLine("¡Operación SELECT ejecutada correctamente!");
                    return (OperationStatus.Success, resultBuilder.ToString());
                }
            }
            catch (Exception ex)
            {
                // Log de errores
                Console.WriteLine($"Error reading file: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                return (OperationStatus.Error, $"Error: {ex.Message}");
            }
        }
        public OperationStatus CreateIndexes(string indexName, string tableName, string columnName, string indexType)
        {
            var CreateIndexesStoreOperation = new CreateIndexesStoreOperation(DataPath, Entities.ConfigPaths.SystemCatalogPath, RutaDeterminadaPorSet);
            return CreateIndexesStoreOperation.Execute(indexName, tableName, columnName, indexType);
        }

        public OperationStatus SelectWhere(){

            return OperationStatus.Success;

        }
    }
}