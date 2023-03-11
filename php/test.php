<?php declare(strict_types=1); error_reporting(-1);

define('DB_HOST', 'db');
define('DB_USER', 'root');
define('DB_PASS', 'mysql');
define('DB_NAME', 'pk_test');

mysqli_report(MYSQLI_REPORT_ALL);
$db = new mysqli(DB_HOST, DB_USER, DB_PASS, DB_NAME);

function _new_pk(): int {
    // mktime(0, 0, 0, 1, 1, 2020) = 1577836800
    return intval((microtime(true) - 1577836800) * 1e10) + rand(1, 1000);
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

function test_32_create_table() {
    global $db;
    $db->query("DROP TABLE IF EXISTS test_32");
    $res = $db->query("CREATE TABLE test_32 (" .
      "id INT UNSIGNED NOT NULL AUTO_INCREMENT, ".
      "name VARCHAR(255) DEFAULT NULL, ".
      "PRIMARY KEY (id) ".
      ")");
    if (!$res || $db->error)
        die("Error: test_32_create_table ${db->error}\n");
}

function test_32_insert_10k() {
    global $db;
    $name = '';
    for ($i = 0; $i < 10e3; $i++) {
        $name = "$i ".sha1($name);
        while (strlen($name) < 120)
            $name .= " ".$name;
        $res = $db->query("INSERT INTO test_32 (name) VALUES ('$name')");
        if (!$res || $db->error)
            die("Error: test_32_insert_10k on $i, error ${db->error}\n");
    }
}

function test_32_bulk_insert_1m() {
    global $db;
    $name = '';
    for ($bi = 0; $bi < 1000; $bi++) {
        $query = "INSERT INTO test_32 (name) VALUES (NULL)";
        for ($i = 1; $i < 1000; $i++) {
            $name = "$bi - $i - ".sha1($name);
            while (strlen($name) < 120)
                $name .= " ".$name;
            $query .= ",('$name')";
        }
        $res = $db->query($query);
        if (!$res || $db->error)
            die("Error: test_32_bulk_insert_1m on $bi - $i, error ${db->error}\n");
    }
}

function test_32_insert_10k_manual() {
    global $db;
    $base = intval(2e6);
    $name = '';
    for ($i = $base; $i < $base+10e3; $i++) {
        $name = "$i ".sha1($name);
        while (strlen($name) < 120)
            $name .= " ".$name;
        $res = $db->query("INSERT INTO test_32 (id, name) VALUES ($i, '$name')");
        if (!$res || $db->error)
            die("Error: test_32_insert_10k_manual on $i, error ${db->error}\n");
    }
}

function test_32_select_random_100k() {
    global $db;
    for ($i = 0; $i < 100e3; $i++) {
        $id = rand(1, (int)1e6);
        $res = $db->query("SELECT id, name FROM test_32 WHERE id=$id");
        $row = $res->fetch_all(MYSQLI_ASSOC);
        if (!$res || empty($row))
            die("Error: test_32_select_random_100k emprty result on $id\n");
    }
}

function test_32_select_sequential_100k() {
    global $db;
    for ($i = 0; $i < 100e3; $i++) {
        $id = $i + 100000;
        $res = $db->query("SELECT id, name FROM test_32 WHERE id=$id");
        $row = $res->fetch_all(MYSQLI_ASSOC);
        if (!$res || empty($row))
            die("Error: test_32_select_sequential_100k emprty result on $id\n");
    }
}

function test_32_update_random_10k() {
    global $db;
    for ($i = 0; $i < 10e3; $i++) {
        $id = rand(1, (int)1e6);
        $name = "$i updated $i updated $i";
        $res = $db->query("UPDATE test_32 SET name='$name' WHERE id=$id");
        if (!$res || $db->affected_rows == 0)
            die("Error: test_32_update_random_10k no affected_rows on $id\n");
    }
}

function test_32_update_sequential_10k() {
    global $db;
    for ($i = 0; $i < 10e3; $i++) {
        $id = $i + 200000;
        $name = "$i updated $i updated $i";
        $res = $db->query("UPDATE test_32 SET name='$name' WHERE id=$id");
        if (!$res || $db->affected_rows == 0)
            die("Error: test_32_update_sequential_10k no affected_rows on $id\n");
    }
}

function test_32_delete_random_10k() {
    global $db;
    for ($i = 0; $i < 10e3; $i++) {
        $id = rand(1, (int)1e6);
        $res = $db->query("DELETE FROM test_32 WHERE id=$id");
        // if (!$res || $db->affected_rows == 0)
        //    echo("Error: test_32_delete_random_10k no affected_rows on $id\n");
    }
}

function test_32_delete_sequential_10k() {
    global $db;
    for ($i = 0; $i < 10e3; $i++) {
        $id = $i + 300000;
        $res = $db->query("DELETE FROM test_32 WHERE id=$id");
        if (!$res || $db->affected_rows == 0)
            echo("Error: test_32_delete_sequential_10k no affected_rows on $id\n");
    }
}

//////////////////// 64 ////////////////////

function test_64_create_table() {
    global $db, $__known_pk;
    $__known_pk = array();
    $db->query("DROP TABLE IF EXISTS test_64");
    $res = $db->query("CREATE TABLE test_64 (" .
      "id BIGINT UNSIGNED NOT NULL, ".
      "name VARCHAR(255) DEFAULT NULL, ".
      "PRIMARY KEY (id) ".
      ")");
    if (!$res || $db->error)
        die("Error: test_32_create_table ${db->error}\n");
}

function test_64_insert_10k() {
    global $db;
    $name = '';
    for ($i = 0; $i < 10e3; $i++) {
        $name = "$i ".sha1($name);
        while (strlen($name) < 120)
            $name .= " ".$name;
        $id = __new_pk();
        $res = $db->query("INSERT INTO test_64 (id, name) VALUES ($id, '$name')");
        if (!$res || $db->error)
            die("Error: test_64_insert_10k on $i, error ${db->error}\n");
    }
}

function test_64_bulk_insert_1m() {
    global $db;
    $name = '';
    for ($bi = 0; $bi < 1000; $bi++) {
        $id = __new_pk();
        $query = "INSERT INTO test_64 (id, name) VALUES ($id, NULL)";
        for ($i = 1; $i < 1000; $i++) {
            $name = "$bi - $i - ".sha1($name);
            while (strlen($name) < 120)
                $name .= " ".$name;
            $id = __new_pk();
            $query .= ",($id, '$name')";
        }
        $res = $db->query($query);
        if (!$res || $db->error)
            die("Error: test_64_bulk_insert_1m on $bi - $i, error ${db->error}\n");
    }
}

function test_64_insert_10k_manual() {
    global $db;
    $base = intval(2e6);
    $name = '';
    for ($i = $base; $i < $base+10e3; $i++) {
        $name = "$i ".sha1($name);
        while (strlen($name) < 120)
            $name .= " ".$name;
        $id = __new_pk();
        $res = $db->query("INSERT INTO test_64 (id, name) VALUES ($id, '$name')");
        if (!$res || $db->error)
            die("Error: test_64_insert_10k_manual on $i, error ${db->error}\n");
    }
}

function test_64_select_random_100k() {
    global $db;
    for ($i = 0; $i < 100e3; $i++) {
        $id = rand_pk(1, (int)1e6);
        $res = $db->query("SELECT id, name FROM test_64 WHERE id=$id");
        $row = $res->fetch_all(MYSQLI_ASSOC);
        if (!$res || empty($row))
            die("Error: test_64_select_random_100k emprty result on $id\n");
    }
}

function test_64_select_sequential_100k() {
    global $db;
    global $__known_pk;
    sort($__known_pk);
    for ($i = 0; $i < 100e3; $i++) {
        $id = $__known_pk[$i];
        $res = $db->query("SELECT id, name FROM test_64 WHERE id=$id");
        $row = $res->fetch_all(MYSQLI_ASSOC);
        if (!$res || empty($row))
            die("Error: test_64_select_sequential_100k emprty result on $id\n");
    }
}

function test_64_update_random_10k() {
    global $db;
    for ($i = 0; $i < 10e3; $i++) {
        $id = rand_pk(1, (int)1e6);
        $name = "$i updated $i updated $i";
        $res = $db->query("UPDATE test_64 SET name='$name' WHERE id=$id");
        if (!$res || $db->affected_rows == 0)
            die("Error: test_64_update_random_10k no affected_rows on $id\n");
    }
}

function test_64_update_sequential_10k() {
    global $db;
    global $__known_pk;
    sort($__known_pk);
    for ($i = 0; $i < 10e3; $i++) {
        $id = $__known_pk[$i];
        $name = "$i updated $i updated $i";
        $res = $db->query("UPDATE test_64 SET name='$name' WHERE id=$id");
        if (!$res || $db->affected_rows == 0)
            die("Error: test_64_update_sequential_10k no affected_rows on $id\n");
    }
}

function test_64_delete_random_10k() {
    global $db;
    for ($i = 0; $i < 10e3; $i++) {
        $id = rand_pk(1, (int)1e6);
        $res = $db->query("DELETE FROM test_64 WHERE id=$id");
        // if (!$res || $db->affected_rows == 0)
        //    echo("Error: test_64_delete_random_10k no affected_rows on $id\n");
    }
}

function test_64_delete_sequential_10k() {
    global $db;
    global $__known_pk;
    for ($i = 0; $i < 10e3; $i++) {
        $id = $__known_pk[$i];
        $res = $db->query("DELETE FROM test_64 WHERE id=$id");
        if (!$res || $db->affected_rows == 0)
            echo("Error: test_64_delete_sequential_10k no affected_rows on $id\n");
    }
}

//////////////////// RUN ////////////////////

function run_suite() {
    echo("=== 32 ===\n");
    run_test('test_32_create_table');
    run_test('test_32_insert_10k');
    run_test('test_32_bulk_insert_1m');
    run_test('test_32_insert_10k_manual');
    run_test('test_32_select_sequential_100k');
    run_test('test_32_update_sequential_10k');
    run_test('test_32_select_random_100k');
    run_test('test_32_update_random_10k');
    run_test('test_32_delete_sequential_10k');
    run_test('test_32_delete_random_10k');

    echo("=== 64 ===\n");
    run_test('test_64_create_table');
    run_test('test_64_insert_10k');
    run_test('test_64_bulk_insert_1m');
    run_test('test_64_insert_10k_manual');
    run_test('test_64_select_sequential_100k');
    run_test('test_64_update_sequential_10k');
    run_test('test_64_select_random_100k');
    run_test('test_64_update_random_10k');
    run_test('test_64_delete_sequential_10k');
    run_test('test_64_delete_random_10k');
}

function main() {
    global $argc, $argv;

    $repeat = $argc > 1 ? intval($argv[1]) : 1;

    for ($n = 0; $n < $repeat; $n++)
        run_suite();

    if ($repeat > 1)
        print_stat();
}

main();
