//扩展方法
$.extend({
    popUp: function (text, type, second = 2) {
        var types = {
            'info': {'background': "#90c24f"},
            'warning': {'background': "#f99e2b"},
            'error': {'background': "#f06a6a"}
        }
        var perfix = {
            'info': 'INFO',
            'warning': 'WARNING',
            'error': 'ERROR'
        }
        type = type in types ? type : 'info'
        $p = $('<p class="popup"></p>').text('{0}：{1}'.format(perfix[type], text)).css({
            'color': 'white',
            'width': '400px',
            'z-index': '5',
            'height': '40px',
            'line-height': '40px',
            'text-align': 'center',
            'position': 'fixed',
            'top': '3px',
            'left': '50%',
            'margin': '3px',
            'margin-left': '-200px',
            'border-radius': '5px'
        }).css(types[type]);
        $('body').append($p);
        setTimeout(() => $('p.popup').remove(), parseInt(second) * 1000)
    },
});

// 扩展数据的属性
String.prototype.format = function (args) {
    var result = this;
    if (arguments.length > 0) {
        if (arguments.length == 1 && typeof (args) == "object") {
            for (var key in args) {
                if (args[key] != undefined) {
                    // noinspection Annotator
                    var reg = new RegExp("({" + key + "})", "g");
                    result = result.replace(reg, args[key]);
                }
            }
        } else {
            for (var i = 0; i < arguments.length; i++) {
                if (arguments[i] != undefined) {
                    // noinspection Annotator
                    var reg = new RegExp("({[" + i + "]})", "g");
                    result = result.replace(reg, arguments[i]);
                }
            }
        }
    }
    return result;
};

$(document).ajaxSend(function () {

});

$(document).ajaxSuccess(function () {
    $.popUp('request success！', 'info', 1)
});

$(document).ajaxError(function () {
    $.popUp('request fail！', 'error', 1)
});