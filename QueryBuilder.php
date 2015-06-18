<?php

namespace yii\db\db2;

class QueryBuilder extends \yii\db\QueryBuilder
{
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
        $orderByStatment = $this->buildOrderBy($orderBy);
        if ($orderByStatment !== '') {
            $sql .= $this->separator . $orderByStatment;
        }

        $limitOffsetStatment = $this->buildLimit($limit, $offset);
        if ($limitOffsetStatment != '') {
            $sql = str_replace(':query', $sql, $limitOffsetStatment);
        }
        return $sql;
    }

    public function buildLimit($limit, $offset)
    {
        if (!$this->hasLimit($limit) && !$this->hasOffset($offset)) {
            return '';
        }

        $limitOffsetStatment = 'SELECT * FROM (SELECT SUBQUERY_.*, ROW_NUMBER() OVER() AS RN_ FROM ( :query ) AS SUBQUERY_) WHERE :offset :limit';

        $replacement = $this->hasOffset($offset) ? 'RN_ > ' . $offset : 'RN_ > 0';
        $limitOffsetStatment = str_replace(':offset', $replacement, $limitOffsetStatment);

        $replacement = $this->hasLimit($limit) ? 'AND RN_ <= ' . ($limit + $offset) : '';
        $limitOffsetStatment = str_replace(':limit', $replacement, $limitOffsetStatment);

        return $limitOffsetStatment;
    }

}
