<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

namespace yii\db\db2;

use yii\db\Exception;
use yii\db\QueryBuilder;
use yii\db\TableSchema;

/**
 * Description of Schema
 *
 * @author Vicente Bissoli Sessa
 */
class Schema extends \yii\db\Schema
{
    public $typeMap = [
        // Character Strings Types
        'character' => self::TYPE_STRING,
        'varchar' => self::TYPE_STRING,
        'clob' => self::TYPE_TEXT,
        // Graphic Strings Types
        'graphic' => self::TYPE_STRING,
        'vargraphic' => self::TYPE_STRING,
        'dbclob' => self::TYPE_TEXT,
        // National Charater Strings Types
        'nchar' => self::TYPE_STRING,
        'nvarchar' => self::TYPE_STRING,
        'nclob' => self::TYPE_TEXT,
        // Binary Strings Types
        'binary' => self::TYPE_BINARY,
        'varbinary' => self::TYPE_BINARY,
        'blob' => self::TYPE_BINARY,
        // Boolean Types
        'boolean' => self::TYPE_BOOLEAN,
        // Numeric Types
        'smallint' => self::TYPE_SMALLINT,
        'int' => self::TYPE_INTEGER,
        'integer' => self::TYPE_INTEGER,
        'bigint' => self::TYPE_BIGINT,
        'decimal' => self::TYPE_DECIMAL,
        'numeric' => self::TYPE_DECIMAL,
        'real' => self::TYPE_FLOAT,
        'float' => self::TYPE_FLOAT,
        'double' => self::TYPE_FLOAT,
        'decfloat' => self::TYPE_FLOAT,
        // Datetime types
        'date' => self::TYPE_DATE,
        'time' => self::TYPE_TIME,
        'timestamp' => self::TYPE_TIMESTAMP
    ];

    public function init()
    {
        parent::init();

        $pdoAttributes = [
            \PDO::ATTR_CASE => \PDO::CASE_LOWER,
            \PDO::ATTR_STRINGIFY_FETCHES => true
        ];

        $this->db->attributes = is_array($this->db->attributes) ? array_merge($this->db->attributes, $pdoAttributes) : $pdoAttributes;

        if (isset($this->defaultSchema)) {
            $this->db->createCommand('SET SCHEMA ' . $this->defaultSchema)->execute();
        }
    }

    public function quoteSimpleTableName($name)
    {
        return strpos($name, '"') !== false ? $name : '"' . $name . '"';
    }

    public function quoteSimpleColumnName($name)
    {
        return strpos($name, '"') !== false || $name === '*' ? $name : '"' . $name . '"';
    }

    public function createQueryBuilder()
    {
        return new QueryBuilder($this->db);
    }

    protected function loadTableSchema($name)
    {
        $table = new TableSchema();
        $this->resolveTableNames($table, $name);

        if ($this->findColumns($table)) {
            $this->findConstraints($table);
            return $table;
        } else {
            return null;
        }
    }

    protected function resolveTableNames($table, $name)
    {
        $parts = explode('.', str_replace('"', '', $name));
        if (isset($parts[1])) {
            $table->schemaName = $parts[0];
            $table->name = $parts[1];
            $table->fullName = $table->schemaName . '.' . $table->name;
        } else {
            $table->fullName = $table->name = $parts[0];
        }
    }

    protected function loadColumnSchema($info)
    {
        $column = $this->createColumnSchema();

        $column->name = $info['name'];
        $column->dbType = $info['dbtype'];
        $column->defaultValue = $info['defaultvalue'];
        $column->scale = $info['scale'];
        // Type precision is given by 'length';
        $column->size = $info['size'];
        $column->precision = $info['size'];
        $column->allowNull = $info['allownull'] === '1';
        $column->isPrimaryKey = $info['isprimarykey'] === '1';
        $column->autoIncrement = $info['autoincrement'] === '1';
        // DB2 does not support unsigned values.
        $column->unsigned = false;
        $column->type = $this->typeMap[strtolower($info['dbtype'])];
        $column->enumValues = null;
        $column->comment = isset($info['comment']) ? $info['comment'] : null;

        if (preg_match('/(varchar|character|clob|graphic|binary|blob)/i', $info['dbtype'])) {
            $column->dbType .= '(' . $info['size'] . ')';
        } elseif (preg_match('/(decimal|double|real)/i', $info['dbtype'])) {
            $column->dbType .= '(' . $info['size'] . ',' . $info['scale'] . ')';
        }

        $column->phpType = $this->getColumnPhpType($column);

        return $column;
    }

    protected function findColumns($table)
    {
        $sql = <<<SQL
            SELECT
                c.colname AS name,
                c.typename AS dbtype,
                c.default AS defaultvalue,
                c.scale AS scale,
                c.length AS size,
                CASE WHEN c.nulls = 'Y'             THEN 1 ELSE 0 END AS allownull,
                CASE WHEN c.keyseq IS NOT NULL      THEN 1 ELSE 0 END AS isprimarykey,
                CASE WHEN c.identity = 'Y'          THEN 1 ELSE 0 END AS autoincrement,
                c.remarks AS comment
            FROM
                syscat.columns AS c
            WHERE
                c.tabname = :table
SQL;

        if (isset($table->schemaName)) {
            $sql .= ' AND c.tabschema = :schema';
        }

        $sql .= ' ORDER BY c.colno';

        $command = $this->db->createCommand($sql);
        $command->bindValue(':table', strtoupper($table->name));

        if (isset($table->schemaName)) {
            $command->bindValue(':schema', strtoupper($table->schemaName));
        }

        try {
            $columns = $command->queryAll();
        } catch (Exception $e) {
            $previous = $e->getPrevious();
            // table does not exist
            // SQLSTATE 42704 An undefined object or constraint name was detected.
            if ($previous instanceof \PDOException && strpos($previous->getMessage(), 'SQLSTATE[42704') != false) {
                return false;
            }
            throw $e;
        }
        foreach ($columns as $info) {
            $column = $this->loadColumnSchema($info);
            $table->columns[$column->name] = $column;
            if ($column->isPrimaryKey) {
                $table->primaryKey[] = $column->name;
//                if ($column->autoIncrement) {
//                    $table->sequenceName = '';
//                } else {
//                    $this->findSequence($table);
//                }
            }
        }
        return true;
    }

    private function findSequence($table)
    {
        $sql = <<<SQL
            SELECT
                s.seqname as sequence
            FROM
                syscat.sequences AS s
            INNER JOIN
                syscat.tables AS t ON s.seqschema = t.tabschema AND s.create_time = t.create_time
            INNER JOIN
                syscat.columns AS c ON t.tabschema = c.tabschema AND t.tabname = c.tabname
            WHERE
                c.tabname = :table AND
                c.identity = 'Y'
SQL;
        if (isset($table->schemaName)) {
            $sql .= ' AND c.tabschema = :schema';
        }

        $command = $this->db->createCommand($sql);
        $command->bindValue(':table', $table->name);

        if (isset($table->schemaName)) {
            $command->bindValue(':schema', $table->schemaName);
        }

        $sequence = $command->queryColumn();
        $table->sequenceName = $sequence;
    }

    protected function findConstraints($table)
    {
        $sql = <<<SQL
            SELECT
                pk.tabname AS tablename,
                fk.colname AS fk,
                pk.colname AS pk
            FROM
                syscat.references AS ref
            INNER JOIN
                syscat.keycoluse AS fk ON ref.constname = fk.constname
            INNER JOIN
                syscat.keycoluse AS pk ON ref.refkeyname = pk.constname AND pk.colseq = fk.colseq
            WHERE
                fk.tabname = :table
SQL;

        if (isset($table->schemaName)) {
            $sql .= ' AND fk.tabschema = :schema';
        }

        $command = $this->db->createCommand($sql);
        $command->bindValue(':table', strtoupper($table->name));

        if (isset($table->schemaName)) {
            $command->bindValue(':schema', strtoupper($table->schemaName));
        }

        $results = $command->queryAll();
        /*
         * result format:
         * [
         *      'tablename' => 'foreignTableName1',
         *      'fk' => 'fk1,
         *      'pk' => 'pk1',
         * ],[
         *      'tablename' => 'foreignTableName2',
         *      'fk' => 'fk2',
         *      'pk' => 'pk2',
         * ], [
         * ...
         * ]
         */
        $foreignKeys = [];
        foreach ($results as $result) {
            $tablename = $result['tablename'];
            $fk = $result['fk'];
            $pk = $result['pk'];
            /*
             * intermediary format:
             * [
             *      'foreignTableName' => [
             *          'fk1' => 'pk1',
             *          'fk2' => 'pk2',
             *          ...
             *      ]
             * ]
             */
            $foreignKeys[$tablename][$fk] = $pk;
        }
        foreach ($foreignKeys as $tablename => $constraints) {
            /*
             * final format:
             * [
             *      'foreignTableName',
             *      'fk1' => 'pk1',
             *      'fk2' => 'pk2',
             *      ...
             * ], [
             *      ...
             * ]
             */
            $foreignKey = [$tablename];
            foreach ($constraints as $fk => $pk) {
                $foreignKey[$fk] = $pk;
            }
            $table->foreignKeys[] = $foreignKey;
        }
    }

    public function findUniqueIndexes($table)
    {
//        [
//          'IndexName1' => ['col1' [, ...]],
//          'IndexName2' => ['col2' [, ...]],
//        ]
        $sql = <<<SQL
            SELECT
                i.indname AS indexname,
                ic.colname AS column
            FROM
                syscat.indexes AS i
            INNER JOIN
                syscat.indexcoluse AS ic ON i.indname = ic.indname
            WHERE
                i.tabname = :table
SQL;

        if (isset($table->schemaName)) {
            $sql .= ' AND tabschema = :schema';
        }

        $sql .= ' ORDER BY ic.colseq';

        $command = $this->db->createCommand($sql);
        $command->bindValue(':table', strtoupper($table->name));

        if (isset($table->schemaName)) {
            $command->bindValue(':schema', strtoupper($table->schemaName));
        }

        $results = $command->queryAll();
        /*  [
         *      'indexname',
         *      'col1',
         *  ], [
         *      'indexname',
         *      'col2'
         *  ]
         */
        $indexes = [];
        foreach ($results as $result) {
            /*
             * [
             *      'indexname' => ['col1', 'col2', ...]
             * ]
             */
            $indexes[$result['indexname']][] = $result['column'];
        }
        return $indexes;
    }

    protected function findTableNames($schema = '')
    {
        $sql = <<<SQL
            SELECT
                t.tabname
            FROM
                syscat.tables AS t
            WHERE
                t.type in ('T', 'V') AND
                t.ownertype != 'S'
SQL;
        if ($schema !== '') {
            $sql .= ' AND t.tabschema = :schema';
        }

        $command = $this->db->createCommand($sql);

        if ($schema !== '') {
            $command->bindParam(':schema', $schema);
        }

        return $command->queryColumn();
    }

}
