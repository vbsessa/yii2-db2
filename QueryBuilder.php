<?php

class QueryBuilder extends \yii\db\QueryBuilder
{
    public $_mysql;
    public $typeMap = [
        Schema::TYPE_PK => 'integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)',
        Schema::TYPE_BIGPK => 'bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)',
        Schema::TYPE_STRING => 'varchar(255)',
        Schema::TYPE_TEXT => 'clob',
        Schema::TYPE_SMALLINT => 'smallint',
        Schema::TYPE_INTEGER => 'integer',
        Schema::TYPE_BIGINT => 'bigint',
        Schema::TYPE_FLOAT => 'float',
        Schema::TYPE_DOUBLE => 'double',
        Schema::TYPE_DECIMAL => 'decimal(10,0)',
        Schema::TYPE_DATETIME => 'datetime',
        Schema::TYPE_TIMESTAMP => 'timestamp',
        Schema::TYPE_TIME => 'time',
        Schema::TYPE_DATE => 'date',
        Schema::TYPE_BINARY => 'blob',
        Schema::TYPE_BOOLEAN => 'char(1)',
        Schema::TYPE_MONEY => 'decimal(19,4)',
    ];

    public function resetSequence($table, $value = null)
    {
        if ($table->sequenceName !== null && is_string($table->primaryKey) && $table->columns[$table->primaryKey]->autoIncrement) {
            if ($value === null) {
                $value = $this->db->createCommand("SELECT MAX({$table->primaryKey}) FROM {$table->fullName}")->queryScalar() + 1;
            } else {
                $value = (int) $value;
            }
            $this->db->createCommand("ALTER TABLE {$table->fullName} ALTER COLUMN {$table->primaryKey} RESTART WITH $value")->execute();
        }
    }

    public function checkIntegrity($check = true, $schema = '', $table = '')
    {
        $enable = $check ? 'CHECKED' : 'UNCHECKED';
        $tableNames = $this->getTableNames($schema);
        $db = $this->getDbConnection();
        foreach ($tableNames as $tableName) {
            $db->createCommand("SET INTEGRITY FOR $tableName ALL IMMEDIATE $enable")->execute();
        }
    }

    public function buildOrderByAndLimit($sql, $orderBy, $limit, $offset)
    {
        $orderBy = $this->buildOrderBy($orderBy);
        if ($orderBy !== '') {
            $sql .= $this->separator . $orderBy;
        }

        $limit = $this->buildLimit($limit, $offset);
        if ($limit != '') {
            $sql = str_replace(':original_query', $limit, $sql);
        }
        return $sql;
    }

    public function buildLimit($limit, $offset)
    {
        if (!$this->hasLimit($limit) && !$this->hasOffset($offset)) {
            return '';
        }

        $sql = 'SELECT * FROM (SELECT _SUBQUERY.*, ROW_NUMBER() OVER() AS _RN FROM ( :original_query ) AS _SUBQUERY) WHERE :offset_condition :limit_condition';

        $sql = $this->hasOffset($offset) ? str_replace(':offset_condition', '_RN > ' . $offset, $sql) : str_replace(':offset_condition', '_RN > 0', $sql);
        $sql = $this->hasLimit($limit) ? str_replace(':limit_condition', 'AND _RN <= ' . ($limit + $offset), $sql) : str_replace(':limit_condition', '', $sql);

        return $sql;
    }

}
