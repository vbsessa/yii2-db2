## IBM DB2 LUW for Yii2

#### Usage

1. Install `pdo_ibm` extension.
2. Configuration in `db.php`:

        return [
            'class' => 'yii\db\Connection',
            'dsn' => 'ibm:DRIVER={IBM DB2 ODBC DRIVER};DATABASE=database;HOSTNAME=hostname;PORT=50000;PROTOCOL=TCPIP;UID=username;PWD=password',
            'username' => 'username',
            'password' => 'password',
            'charset' => 'utf8',
            'schemaMap' => [
                'ibm' => [
                    'class' => 'yii\db\db2\Schema',
                    'defaultSchema' => 'SAMPLE'
                ]
            ]
        ];

3. :)
