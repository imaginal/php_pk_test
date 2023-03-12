<?php declare(strict_types=1); error_reporting(-1);

define('PDO_DSN', 'pgsql:host=db;dbname=pk_test');
//define('PDO_DSN', 'mysql:host=db;dbname=pk_test');
define('PDO_USER', 'pkuser');
define('PDO_PASS', 'pksecret');

define('INSERT_MANUAL',     (int)  10e3);
define('INSERT_BULK',       (int)   100);
define('SELECT_SEQUENTIAL', (int) 100e3);
define('SELECT_RANDOM',     (int) 100e3);
define('UPDATE_SEQUENTIAL', (int)  10e3);
define('UPDATE_RANDOM',     (int)  10e3);
define('DELETE_SEQUENTIAL', (int)  10e3);
define('DELETE_RANDOM',     (int)  10e3);

$db = NULL;

set_error_handler(function ($severity, $msg, $file, $line) {
    throw new ErrorException($msg, 0, $severity, $file, $line);
});

$__pk_offset = 0;

function _new_pk(): int {
    global $__pk_offset;
    $__pk_offset += rand(1, (int)1e10);
    // mktime(0, 0, 0, 1, 1, 2020) = 1577836800
    return intval((microtime(true) - 1577836800) * 1e10) + rand(1, (int)1e5) + $__pk_offset;
}

$__known_pk = array();

function __new_pk(): int {
    global $__known_pk;
    $pk = _new_pk();
    $__known_pk[] = $pk;
    return $pk;
}

function rand_pk($a, $b): int {
    global $__known_pk;
    $max = count($__known_pk);
    $i = rand($a, $b);
    if ($i >= $max)
        $i = $i % $max;
    return $__known_pk[$i];
}

$__uid_id = rand();

function _new_uid(): string {
    global $__uid_id;
    $__uid_id += 1;
    return md5(microtime() . uniqid("", true) . $__uid_id);
}

$__known_uid = array();

function __new_uid(): string {
    global $__known_uid;
    $pk = _new_uid();
    $__known_uid[] = $pk;
    return $pk;
}

function rand_uid($a, $b): string {
    global $__known_uid;
    $i = rand($a, $b);
    $max = count($__known_uid);
    if ($i >= $max)
        $i = $i % $max;
    return $__known_uid[$i];
}

$__test_stat = array();

function run_test(string $func) {
    global $__test_stat;
    $t = microtime(true);
    call_user_func($func);
    $t = microtime(true) - $t;
    printf("%6.3f\t%s\n", $t, $func);
    if (empty($__test_stat[$func]))
        $__test_stat[$func] = 0.0;
    $__test_stat[$func] += $t;
}

function print_stat() {
    global $__test_stat;
    echo("=== TOTAL ===\n");
    foreach ($__test_stat as $func => $t)
        printf("%6.3f\t%s\n", $t, $func);
}

//////////////////// 32 ////////////////////

function test_32_create_table_mysql() {
    global $db;
    $db->query("DROP TABLE IF EXISTS test_32");
    $res = $db->query("CREATE TABLE test_32 (" .
      "id INT UNSIGNED NOT NULL AUTO_INCREMENT, ".
      "name VARCHAR(255) DEFAULT NULL, ".
      "PRIMARY KEY (id) ".
      ")");
    if (!$res || $db->errorCode() != "00000") {
        $error = $db->errorCode();
        die("Error: test_32_create_table {$error}\n");
    }
}

function test_32_create_table_pgsql() {
    global $db;
    $db->query("DROP TABLE IF EXISTS test_32");
    $res = $db->query("CREATE TABLE test_32 (" .
      "id SERIAL,".
      "name VARCHAR(255) DEFAULT NULL, ".
      "PRIMARY KEY (id) ".
      ")");
    if (!$res || $db->errorCode() != "00000") {
        $error = $db->errorCode();
        die("Error: test_32_create_table {$error}\n");
    }
}

function test_32_create_table() {
    if (substr(PDO_DSN, 0, 5) == "mysql")
        return test_32_create_table_mysql();
    else if (substr(PDO_DSN, 0, 5) == "pgsql")
        return test_32_create_table_pgsql();
    die("Error: Unknown server type\n");
}

function test_32_insert_auto() {
    global $db;
    $name = '';
    for ($i = 0; $i < INSERT_MANUAL; $i++) {
        $name = "$i - ".sha1($name);
        while (strlen($name) < 120)
            $name .= " / ".$name;
        $res = $db->query("INSERT INTO test_32 (name) VALUES ('$name')");
        if (!$res || $db->errorCode() != "00000") {
            $error = $db->errorCode();
            die("Error: test_32_insert_auto on $i, error {$error}\n");
        }
    }
}

function test_32_bulk_insert() {
    global $db;
    $name = '';
    for ($bi = 0; $bi < INSERT_BULK; $bi++) {
        $query = "INSERT INTO test_32 (name) VALUES (NULL)";
        for ($i = 1; $i < INSERT_MANUAL; $i++) {
            $name = "$bi - $i - ".sha1($name);
            while (strlen($name) < 120)
                $name .= " / ".$name;
            $query .= ",('$name')";
        }
        $res = $db->query($query);
        if (!$res || $db->errorCode() != "00000") {
            $error = $db->errorCode();
            die("Error: test_32_bulk_insert on $bi - $i, error {$error}\n");
        }
    }
}

function test_32_insert_manual() {
    global $db;
    $base = intval(2e6);
    $name = '';
    for ($i = 0; $i < INSERT_MANUAL; $i++) {
        $name = "$base - $i - ".sha1($name);
        while (strlen($name) < 120)
            $name .= " / ".$name;
        $id = $base + $i;
        $res = $db->query("INSERT INTO test_32 (id, name) VALUES ($id, '$name')");
        if (!$res || $db->errorCode() != "00000") {
            $error = $db->errorCode();
            die("Error: test_32_insert_manual on $i, error {$error}\n");
        }
    }
}

function test_32_select_random() {
    global $db;
    for ($i = 0; $i < SELECT_RANDOM; $i++) {
        $id = rand(1, intval(INSERT_MANUAL*(INSERT_BULK+1)));
        $res = $db->query("SELECT id, name FROM test_32 WHERE id=$id");
        $row = $res->fetch();
        if (!$res || !$row)
            die("Error: test_32_select_random emprty result on $id\n");
    }
}

function test_32_select_sequential() {
    global $db;
    for ($i = 0; $i < SELECT_SEQUENTIAL; $i++) {
        $id = $i + INSERT_MANUAL;
        $res = $db->query("SELECT id, name FROM test_32 WHERE id=$id");
        $row = $res->fetch();
        if (!$res || !$row)
            die("Error: test_32_select_sequential emprty result on $id\n");
    }
}

function test_32_update_random() {
    global $db;
    for ($i = 0; $i < UPDATE_RANDOM; $i++) {
        $id = rand(1, intval(INSERT_MANUAL*(INSERT_BULK+1)));
        $name = "$i updated $i updated $i random ".rand();
        $res = $db->query("UPDATE test_32 SET name='$name' WHERE id=$id");
        if (!$res || $res->rowCount() == 0)
            die("Error: test_32_update_random no affected_rows on $id\n");
    }
}

function test_32_update_sequential() {
    global $db;
    for ($i = 0; $i < UPDATE_SEQUENTIAL; $i++) {
        $id = $i + 2 * INSERT_MANUAL;
        $name = "$i updated $i updated $i random ".rand();
        $res = $db->query("UPDATE test_32 SET name='$name' WHERE id=$id");
        if (!$res || $res->rowCount() == 0)
            die("Error: test_32_update_sequential no affected_rows on $id\n");
    }
}

function test_32_delete_random() {
    global $db;
    $errors = 0;
    for ($i = 0; $i < DELETE_RANDOM; $i++) {
        $id = rand(1, intval(INSERT_MANUAL*(INSERT_BULK+1)));
        $res = $db->query("DELETE FROM test_32 WHERE id=$id");
        if (!$res || $res->rowCount() == 0)
            $errors += 1;
        if ($errors > DELETE_RANDOM/10)
            die("Error: test_32_delete_random $errors no affected_rows on $id\n");
    }
}

function test_32_delete_sequential() {
    global $db;
    for ($i = 0; $i < DELETE_SEQUENTIAL; $i++) {
        $id = $i + 3 * INSERT_MANUAL;
        $res = $db->query("DELETE FROM test_32 WHERE id=$id");
        if (!$res || $res->rowCount() == 0)
            die("Error: test_32_delete_sequential no affected_rows on $id\n");
    }
}

//////////////////// 64 ////////////////////

function test_64_create_table_mysql() {
    global $db, $__known_pk;
    $__known_pk = array();
    $db->query("DROP TABLE IF EXISTS test_64");
    $res = $db->query("CREATE TABLE test_64 (" .
      "id BIGINT UNSIGNED NOT NULL, ".
      "name VARCHAR(255) DEFAULT NULL, ".
      "PRIMARY KEY (id) ".
      ")");
    if (!$res || $db->errorCode() != "00000") {
        $error = $db->errorCode();
        die("Error: test_64_create_table {$error}\n");
    }
}

function test_64_create_table_pgsql() {
    global $db, $__known_pk;
    $__known_pk = array();
    $db->query("DROP TABLE IF EXISTS test_64");
    $res = $db->query("CREATE TABLE test_64 (" .
      "id INT8 NOT NULL, ".
      "name VARCHAR(255) DEFAULT NULL, ".
      "PRIMARY KEY (id) ".
      ")");
    if (!$res || $db->errorCode() != "00000") {
        $error = $db->errorCode();
        die("Error: test_64_create_table {$error}\n");
    }
}

function test_64_create_table() {
    if (substr(PDO_DSN, 0, 5) == "mysql")
        return test_64_create_table_mysql();
    else if (substr(PDO_DSN, 0, 5) == "pgsql")
        return test_64_create_table_pgsql();
    die("Error: Unknown server type\n");
}

function test_64_bulk_insert() {
    global $db;
    $name = '';
    for ($bi = 0; $bi < INSERT_BULK; $bi++) {
        $id = __new_pk();
        $query = "INSERT INTO test_64 (id, name) VALUES ($id, NULL)";
        for ($i = 1; $i < INSERT_MANUAL; $i++) {
            $name = "$bi - $i - ".sha1($name);
            while (strlen($name) < 120)
                $name .= " / ".$name;
            $id = __new_pk();
            $query .= ",($id, '$name')";
        }
        $res = $db->query($query);
        if (!$res || $db->errorCode() != "00000") {
            $error = $db->errorCode();
            die("Error: test_64_bulk_insert on $bi - $i, error {$error}\n");
        }
    }
}

function test_64_insert_manual() {
    global $db;
    $name = '';
    for ($i = 0; $i < INSERT_MANUAL; $i++) {
        $name = "$i - ".sha1($name);
        while (strlen($name) < 120)
            $name .= " / ".$name;
        $id = __new_pk();
        $res = $db->query("INSERT INTO test_64 (id, name) VALUES ($id, '$name')");
        if (!$res || $db->errorCode() != "00000") {
            $error = $db->errorCode();
            die("Error: test_64_insert_manual on $i, error {$error}\n");
        }
    }
}

function test_64_select_random() {
    global $db;
    for ($i = 0; $i < SELECT_RANDOM; $i++) {
        $id = rand_pk(1, intval(INSERT_MANUAL*(INSERT_BULK+1)));
        $res = $db->query("SELECT id, name FROM test_64 WHERE id=$id");
        $row = $res->fetch();
        if (!$res || !$row)
            die("Error: test_64_select_random emprty result on $id\n");
    }
}

function test_64_select_sequential() {
    global $db;
    global $__known_pk;
    sort($__known_pk);
    for ($i = 0; $i < SELECT_SEQUENTIAL; $i++) {
        $id = $__known_pk[$i];
        $res = $db->query("SELECT id, name FROM test_64 WHERE id=$id");
        $row = $res->fetch();
        if (!$res || !$row)
            die("Error: test_64_select_sequential emprty result on $id\n");
    }
}

function test_64_update_random() {
    global $db;
    for ($i = 0; $i < UPDATE_RANDOM; $i++) {
        $id = rand_pk(1, intval(INSERT_MANUAL*(INSERT_BULK+1)));
        $name = "$i updated $i updated $i random ".rand();
        $res = $db->query("UPDATE test_64 SET name='$name' WHERE id=$id");
        if (!$res || $res->rowCount() == 0)
            die("Error: test_64_update_random no affected_rows on $id\n");
    }
}

function test_64_update_sequential() {
    global $db;
    global $__known_pk;
    sort($__known_pk);
    for ($i = 0; $i < UPDATE_SEQUENTIAL; $i++) {
        $id = $__known_pk[$i];
        $name = "$i updated $i updated $i random ".rand();
        $res = $db->query("UPDATE test_64 SET name='$name' WHERE id=$id");
        if (!$res || $res->rowCount() == 0)
            die("Error: test_64_update_sequential no affected_rows on $id\n");
    }
}

function test_64_delete_random() {
    global $db;
    $errors = 0;
    for ($i = 0; $i < DELETE_RANDOM; $i++) {
        $id = rand_pk(1, intval(INSERT_MANUAL*(INSERT_BULK+1)));
        $res = $db->query("DELETE FROM test_64 WHERE id=$id");
        if (!$res || $res->rowCount() == 0)
            $errors += 1;
        if ($errors > DELETE_RANDOM/10)
            die("Error: test_64_delete_random $errors no affected_rows on $id\n");
    }
}

function test_64_delete_sequential() {
    global $db;
    global $__known_pk;
    for ($i = 0; $i < DELETE_SEQUENTIAL; $i++) {
        $id = $__known_pk[$i];
        $res = $db->query("DELETE FROM test_64 WHERE id=$id");
        if (!$res || $res->rowCount() == 0)
            die("Error: test_64_delete_sequential no affected_rows on $id\n");
    }
}

//////////////////// UID ////////////////////

function test_uid_create_table_mysql() {
    global $db, $__known_uid;
    $__known_uid = array();
    $db->query("DROP TABLE IF EXISTS test_uid");
    $res = $db->query("CREATE TABLE test_uid (" .
      "id VARCHAR(32) NOT NULL, ".
      "name VARCHAR(255) DEFAULT NULL, ".
      "PRIMARY KEY (id) ".
      ")");
    if (!$res || $db->errorCode() != "00000") {
        $error = $db->errorCode();
        die("Error: test_uid_create_table {$error}\n");
    }
}

function test_uid_create_table() {
    return test_uid_create_table_mysql();
}

function test_uid_bulk_insert() {
    global $db;
    $name = '';
    for ($bi = 0; $bi < INSERT_BULK; $bi++) {
        $id = __new_uid();
        $query = "INSERT INTO test_uid (id, name) VALUES ('$id', NULL)";
        for ($i = 1; $i < INSERT_MANUAL; $i++) {
            $name = "$bi - $i - ".sha1($name);
            while (strlen($name) < 120)
                $name .= " ".$name;
            $id = __new_uid();
            $query .= ",('$id', '$name')";
        }
        $res = $db->query($query);
        if (!$res || $db->errorCode() != "00000") {
            $error = $db->errorCode();
            die("Error: test_uid_bulk_insert on $bi - $i, error {$error}\n");
        }
    }
}

function test_uid_insert_manual() {
    global $db;
    $name = '';
    for ($i = 0; $i < INSERT_MANUAL; $i++) {
        $name = "$i - ".sha1($name);
        while (strlen($name) < 120)
            $name .= " / ".$name;
        $id = __new_uid();
        $res = $db->query("INSERT INTO test_uid (id, name) VALUES ('$id', '$name')");
        if (!$res || $db->errorCode() != "00000") {
            $error = $db->errorCode();
            die("Error: test_uid_insert_manual on $i, error {$error}\n");
        }
    }
}

function test_uid_select_random() {
    global $db;
    for ($i = 0; $i < SELECT_RANDOM; $i++) {
        $id = rand_uid(1, intval(INSERT_MANUAL*(INSERT_BULK+1)));
        $res = $db->query("SELECT id, name FROM test_uid WHERE id='$id'");
        $row = $res->fetch();
        if (!$res || !$row)
            die("Error: test_uid_select_random emprty result on $id\n");
    }
}

function test_uid_select_sequential() {
    global $db;
    global $__known_uid;
    sort($__known_uid);
    for ($i = 0; $i < SELECT_SEQUENTIAL; $i++) {
        $id = $__known_uid[$i];
        $res = $db->query("SELECT id, name FROM test_uid WHERE id='$id'");
        $row = $res->fetch();
        if (!$res || !$row)
            die("Error: test_uid_select_sequential emprty result on $id\n");
    }
}

function test_uid_update_random() {
    global $db;
    for ($i = 0; $i < UPDATE_RANDOM; $i++) {
        $id = rand_uid(1, intval(INSERT_MANUAL*(INSERT_BULK+1)));
        $name = "$i updated $i updated $i random ".rand();
        $res = $db->query("UPDATE test_uid SET name='$name' WHERE id='$id'");
        if (!$res || $res->rowCount() == 0)
            die("Error: test_uid_update_random no affected_rows on $id\n");
    }
}

function test_uid_update_sequential() {
    global $db;
    global $__known_uid;
    sort($__known_uid);
    for ($i = 0; $i < UPDATE_SEQUENTIAL; $i++) {
        $id = $__known_uid[$i];
        $name = "$i updated $i updated $i random ".rand();
        $res = $db->query("UPDATE test_uid SET name='$name' WHERE id='$id'");
        if (!$res || $res->rowCount() == 0)
            die("Error: test_uid_update_sequential no affected_rows on $id\n");
    }
}

function test_uid_delete_random() {
    global $db;
    $errors = 0;
    for ($i = 0; $i < DELETE_RANDOM; $i++) {
        $id = rand_uid(1, intval(INSERT_MANUAL*(INSERT_BULK+1)));
        $res = $db->query("DELETE FROM test_uid WHERE id='$id'");
        if (!$res || $res->rowCount() == 0)
            $errors += 1;
        if ($errors > DELETE_RANDOM/10)
            die("Error: test_uid_delete_random $errors no affected_rows on $id\n");
    }
}

function test_uid_delete_sequential() {
    global $db;
    global $__known_uid;
    for ($i = 0; $i < DELETE_SEQUENTIAL; $i++) {
        $id = $__known_uid[$i];
        $res = $db->query("DELETE FROM test_uid WHERE id='$id'");
        if (!$res || $res->rowCount() == 0)
            die("Error: test_uid_delete_sequential no affected_rows on $id\n");
    }
}

//////////////////// RUN ////////////////////

function print_settings() {
    echo("INSERT_MANUAL ........: ".INSERT_MANUAL."\n");
    echo("INSERT_BULK ..........: ".INSERT_BULK." x ".INSERT_MANUAL."\n");
    echo("SELECT_SEQUENTIAL ....: ".SELECT_SEQUENTIAL."\n");
    echo("SELECT_RANDOM ........: ".SELECT_RANDOM."\n");
    echo("UPDATE_SEQUENTIAL ....: ".UPDATE_SEQUENTIAL."\n");
    echo("UPDATE_RANDOM ........: ".UPDATE_RANDOM."\n");
    echo("DELETE_SEQUENTIAL ....: ".DELETE_SEQUENTIAL."\n");
    echo("DELETE_RANDOM ........: ".DELETE_RANDOM."\n\n");
}

function run_suite() {
    echo("=== 32 ===\n");
    run_test('test_32_create_table');
    run_test('test_32_insert_auto');
    run_test('test_32_bulk_insert');
    run_test('test_32_insert_manual');
    run_test('test_32_select_sequential');
    run_test('test_32_update_sequential');
    run_test('test_32_select_random');
    run_test('test_32_update_random');
    run_test('test_32_delete_sequential');
    run_test('test_32_delete_random');

    echo("=== 64 ===\n");
    run_test('test_64_create_table');
    run_test('test_64_insert_manual');
    run_test('test_64_bulk_insert');
    run_test('test_64_insert_manual');
    run_test('test_64_select_sequential');
    run_test('test_64_update_sequential');
    run_test('test_64_select_random');
    run_test('test_64_update_random');
    run_test('test_64_delete_sequential');
    run_test('test_64_delete_random');

    echo("=== UID ===\n");
    run_test('test_uid_create_table');
    run_test('test_uid_insert_manual');
    run_test('test_uid_bulk_insert');
    run_test('test_uid_insert_manual');
    run_test('test_uid_select_sequential');
    run_test('test_uid_update_sequential');
    run_test('test_uid_select_random');
    run_test('test_uid_update_random');
    run_test('test_uid_delete_sequential');
    run_test('test_uid_delete_random');
}

function main() {
    global $db, $argc, $argv;

    $repeat = $argc > 1 ? intval($argv[1]) : 1;

    echo("Repeat ...............: $repeat time/s\n");
    echo("Connect to ...........: ".PDO_DSN."\n\n");

    $db = new PDO(PDO_DSN, PDO_USER, PDO_PASS);
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    print_settings();

    for ($n = 0; $n < $repeat; $n++)
        run_suite();

    if ($repeat > 1)
        print_stat();
}

main();
