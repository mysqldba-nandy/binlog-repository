$(document).ready(function () {

    init();

    function init() {
        resize();
        $('#timezone').val(new Date().getTimezoneOffset() / -60);
        get_files();
    }

    $(window).resize(function () {
        resize()
    });

    function resize() {
        let height = $(window).height() - parseInt($('#sql').css('padding-top')) * 5;
        $('#sql').height(height);
        $('#log').height(height - $('form').height())
    }

    $('#file').on('change', function () {
        let file = $('#file').val();
        get_databases(file)
    });

    $("#database").on('click', function () {
        if ($(this).val() == null) {
            let file = $('#file').val();
            get_databases(file);
        }
    });

    $("#database").on('change', function () {
        let file = $('#file').val();
        let database = $('#database').val();
        get_tables(file, database)
    });

    $('#table').on('click', function () {
        let database = $('#database').val();
        if (database && $('#table').val() == null) {
            let file = $('#file').val();
            get_tables(file, database)
        }
    });

    $("#start").click(function () {
        let _data = {
            'file': $('#file').val(),
            'database': $('#database').val(),
            'start_time': $('#start-time').val(),
            'stop_time': $('#stop-time').val(),
            'timezone': $('#timezone').val(),
            'start_position': $('#start-position').val(),
            'stop_position': $('#stop-position').val(),
            'output_type': $("#output-type input:checked").prop('value')
        };
        let data = {}
        if (_data['output_type'] == 'DDL') {
            data = _data
        } else {
            let sql_type = [];
            let $input = $('#sql-type input:checked');
            for (let i = 0; i < $input.length; i++) {
                sql_type.push($input.eq(i).attr('value'))
            }
            _data['table'] = $('#table').val();
            _data['sql_type'] = JSON.stringify(sql_type);
            _data['use_pk'] = $("#pk-type input:first").prop('checked').toString();
            _data['no_pk'] = $("#pk-type input:last").prop('checked').toString();
            _data['undo'] = $("#output-type input:first").prop('checked').toString();
            for (key in _data) {
                if (_data[key]) {
                    if (['start_time', 'stop_time'].indexOf(key) >= 0) {
                        let unixtime = timestamp(_data[key]);
                        if (unixtime > 0) {
                            data[key] = unixtime
                        }
                    } else {
                        data[key] = _data[key]
                    }
                }
            }
            if (data['sql_type'] == '[]') {
                $.popUp('please choose [INSERT, UPDATE, DELETE]', 'error')
                return
            }
        }
        $.popUp('please wait a moment...', 'warning');
        log('please wait a moment...');
        $.ajax({
            url: "/binlogs",
            type: "post",
            data: data,
            dataType: 'json',
            success: function (data) {
                let $sql = $('#sql');
                $sql.val('');
                log('found {0} rows'.format(data.length));
                let sqls = '';
                for (sql of data) {
                    sqls = sqls + '{0}\n'.format(sql)
                }
                if (sqls) {
                    $sql.val(sqls);
                }
            }
        });
    });

    $('#download').click(function () {
        let filename = '{0}-{1}-{2}.sql'.format(
            $('#database').val(),
            $('#table').val(),
            $("#output-type input:checked").prop('value')
        );
        let sql = $('#sql').val();
        download(filename, sql);
    });

    function get_files() {
        $.ajax({
            url: "/files",
            type: "post",
            dataType: 'json',
            success: function (data) {
                let options = [];
                for (file of data) {
                    options.push('<option>{0}</option>'.format(file))
                }
                $('#file').html(options.join(''));
                log('found {0} files'.format(data.length))
            }
        });
    }

    function get_databases(file) {
        $.ajax({
            url: "/databases",
            type: "post",
            data: {'file': file},
            dataType: 'json',
            success: function (data) {
                let options = [];
                for (database of data) {
                    options.push('<option>{0}</option>'.format(database))
                }
                $('#database').html(options.join(''));
                log('found {0} databases'.format(data.length))
            }
        });
    }

    function get_tables(file, database) {
        $.ajax({
            url: "/tables",
            type: "post",
            data: {'file': file, 'database': database},
            dataType: 'json',
            success: function (data) {
                let options = [];
                for (table of data) {
                    options.push('<option>{0}</option>'.format(table))
                }
                $('#table').html(options.join(''));
                log('found {0} tables in {1}'.format(data.length, $("#database").val()))
            }
        });
    }

    function timestamp(time) {
        let unixtime = parseInt(time);
        switch (unixtime.toString().length) {
            case NaN:
                unixtime = 0;
                break;
            case 10:
                break;
            case 13:
                unixtime = unixtime / 1000;
                break;
            default:
                try {
                    unixtime = new Date(time).getTime() / 1000;
                    let timezone = parseInt($('#timezone').val());
                    if (!timezone) timezone = 0;
                    if (timezone) {
                        unixtime += 3600 * timezone
                    }
                } catch (e) {
                    $.popUp('wrong time format!', 'error');
                    unixtime = 0
                }
        }
        return unixtime
    }

    function download(filename, sql) {
        $a = $('<a></a>');
        $a.attr('href', window.URL.createObjectURL(new Blob([sql])));
        $a.attr('download', filename);
        $('#download').after($a);
        $a[0].click();
        $a.remove();
    }

    function log(log) {
        let $log = $('#log');
        if ($log.val()) {
            $log.val("{0} {1}\n{2}".format(time(), log, $log.val()))
        } else {
            $log.val("{0} {1}".format(time(), log))
        }
    }

    function time() {
        let timezone = parseInt($('#timezone').val());
        if (!timezone) timezone = 0;
        let time = new Date().toISOString().slice(11, 19);
        let hour = parseInt(time.slice(0, 2)) + timezone;
        if (hour > 24) {
            hour -= 24
        } else {
            if (hour < 0) {
                hour += 24
            }
        }
        hour = hour.toString();
        if (hour.length == 1) {
            hour = '0' + hour
        }
        return hour + time.slice(2, 8)
    }
});