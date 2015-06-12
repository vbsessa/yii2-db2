<?php

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
        'character' => self::TYPE_STRING,
        'varchar' => self::TYPE_STRING,
        'clob' => self::TYPE_TEXT,
        'graphic' => self::TYPE_STRING,
        'vargraphic' => self::TYPE_STRING,
        'dbclob' => self::TYPE_TEXT,
        'nchar' => self::TYPE_STRING,
        'nvarchar' => self::TYPE_STRING,
        'nclob' => self::TYPE_TEXT,
        'binary' => self::TYPE_BINARY,
        'varbinary' => self::TYPE_BINARY,
        'blob' => self::TYPE_BINARY,
        'boolean' => self::TYPE_BOOLEAN,
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
        'date' => self::TYPE_DATE,
        'time' => self::TYPE_TIME,
        'timestamp' => self::TYPE_TIMESTAMP
    ];

    /**
     *
     * @inheritdoc
     */
    public function init()
    {
        parent::init();

        $this->db->slavePdo->setAttribute(\PDO::ATTR_CASE, \PDO::CASE_NATURAL);

        if (isset($this->defaultSchema)) {
            $this->db->createCommand('SET SCHEMA ' . $this->quoteSimpleTableName($this->defaultSchema))->execute();
        }
    }

    /**
     *
     * @inheritdoc
     */
    public function quoteSimpleTableName($name)
    {
        return strpos($name, '"') !== false ? $name : '"' . $name . '"';
    }

    /**
     *
     * @inheritdoc
     */
    public function quoteSimpleColumnName($name)
    {
        return strpos($name, '"') !== false || $name === '*' ? $name : '"' . $name . '"';
    }

    /**
     *
     * @inheritdoc
     */
    public function createQueryBuilder()
    {
        return new QueryBuilder($this->db);
    }

    /**
     *
     * @inheritdoc
     */
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

    /**
     *
     * @inheritdoc
     */
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

    /**
     *
     * @inheritdoc
     */
    protected function loadColumnSchema($info)
    {
        $column = $this->createColumnSchema();

        $column->name = $info['name'];
        $column->dbType = $info['dbtype'];
        $column->defaultValue = isset($info['defaultvalue']) ? $info['defaultvalue'] : null;
        $column->scale = $info['scale'];
        $column->size = $info['size'];
        $column->precision = $info['size'];
        $column->allowNull = $info['allownull'] === '1';
        $column->isPrimaryKey = $info['isprimarykey'] === '1';
        $column->autoIncrement = $info['autoincrement'] === '1';
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

    /**
     *
     * @inheritdoc
     */
    protected function findColumns($table)
    {
        $sql = <<<SQL
            SELECT
                c.colname AS name,
                c.typename AS dbtype,
                c.default AS defaultvalue,
                c.scale AS scale,
                c.length AS size,
                CASE WHEN c.nulls = 'Y'         THEN 1 ELSE 0 END AS allownull,
                CASE WHEN c.keyseq IS NOT NULL  THEN 1 ELSE 0 END AS isprimarykey,
                CASE WHEN c.identity = 'Y'      THEN 1 ELSE 0 END AS autoincrement,
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
        $command->bindValue(':table', $table->name);

        if (isset($table->schemaName)) {
            $command->bindValue(':schema', $table->schemaName);
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
            if ($this->db->slavePdo->getAttribute(\PDO::ATTR_CASE) !== \PDO::CASE_LOWER) {
                $info = array_change_key_case($info, CASE_LOWER);
            }
            $column = $this->loadColumnSchema($info);
            $table->columns[$column->name] = $column;
            $table->sequenceName = '';
            if ($column->isPrimaryKey) {
                $table->primaryKey[] = $column->name;
            }
        }
        return true;
    }

    /**
     *
     * @inheritdoc
     */
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
        $command->bindValue(':table', $table->name);

        if (isset($table->schemaName)) {
            $command->bindValue(':schema', $table->schemaName);
        }

        $results = $command->queryAll();
        $foreignKeys = [];
        foreach ($results as $result) {
            if ($this->db->slavePdo->getAttribute(\PDO::ATTR_CASE) !== \PDO::CASE_LOWER) {
                $result = array_change_key_case($result, CASE_LOWER);
            }
            $tablename = $result['tablename'];
            $fk = $result['fk'];
            $pk = $result['pk'];
            $foreignKeys[$tablename][$fk] = $pk;
        }
        foreach ($foreignKeys as $tablename => $keymap) {
            $constraint = [$tablename];
            foreach ($keymap as $fk => $pk) {
                $constraint[$fk] = $pk;
            }
            $table->foreignKeys[] = $constraint;
        }
    }

    /**
     *
     * @inheritdoc
     */
    public function findUniqueIndexes($table)
    {
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
        $command->bindValue(':table', $table->name);

        if (isset($table->schemaName)) {
            $command->bindValue(':schema', $table->schemaName);
        }

        $results = $command->queryAll();
        $indexes = [];
        foreach ($results as $result) {
            if ($this->db->slavePdo->getAttribute(\PDO::ATTR_CASE) !== \PDO::CASE_LOWER) {
                $result = array_change_key_case($result, CASE_LOWER);
            }
            $indexes[$result['indexname']][] = $result['column'];
        }
        return $indexes;
    }

    /**
     *
     * @inheritdoc
     */
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
            $command->bindValue(':schema', $schema);
        }

        return $command->queryColumn();
    }

}
