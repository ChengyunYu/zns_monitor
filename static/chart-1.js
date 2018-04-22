var a = echarts;
var myChart_1 = a.init(document.getElementById('chart-1'));
var myChart_2 = a.init(document.getElementById('chart-2'));
var myChart_3 = a.init(document.getElementById('chart-3'));  
//var myChart_4 = a.init(document.getElementById('chart-4')); 

var setJson = {
    title: {  
         text: 'waiting for query data...'  
     }, 
    tooltip: {
        trigger: 'axis'
    },
    legend: {
        data: ['data for today']
    },
    toolbox: {
        show: true,
        feature: {
            mark: {
                show: true
            },
            dataView: {
                show: true,
                readOnly: false
            },
            magicType: {
                show: true,
                type: ['line', 'bar']
            },
            // restore : {show: true},  
            // saveAsImage : {show: true}  
        }
    },
    calculable: true,

    xAxis: [{
        type: 'category',
        boundaryGap: false,
        data: []
    }],
    yAxis: [{
        type: 'value',
        axisLabel: {
            formatter: '{value}'
        }
    }],
    series: [{
        name: 'max num',
        type: 'line',
        data: [],
        markPoint: {
            data: [{
                    type: 'max',
                    name: 'max val'
                },
                {
                    type: 'min',
                    name: 'max val'
                }
            ]
        },
        markLine: {
            data: [{
                type: 'average',
                name: 'avg'
            }]
        }
    }, ]
};


myChart_1.setOption(setJson);
myChart_2.setOption(setJson);
myChart_3.setOption(setJson);
//myChart_1.showLoading();    

var getting = {
    type: "get",
    url: "http://127.0.0.1:5000/echarts-1",  
    dataType: "json",  
    success: function(result) {
        if (result['type'] === 'topk') {
            var names = [];  
            var nums = [];   
            for (var i = 0; i < result["data"].length; i++) {
                names.push(result["data"][i]["name"]); 
            }
            for (var i = 0; i < result["data"].length; i++) {
                nums.push(result["data"][i]["num"]); 
            }
            myChart_1.hideLoading(); 
            myChart_1.setOption({  
                title: {
                    text: 'top K'
                },
                xAxis: {
                    data: names
                },
                series: [{
                    name: 'nums',
                    data: nums
                }]
            });
        }

    },
    error: function(errorMsg) {
        console.log("request failed!");
        myChart_1.hideLoading();
    }
}
window.setInterval(function() {
    $.ajax(getting)
}, 1000);










var getting_2 = {
    type: "get",
    url: "http://127.0.0.1:5000/echarts-1",  
    dataType: "json", 
    success: function(result) {
        if (result["type"] === 'devx') {
            var names = [];  
            var nums = [];   
            for (var i = 0; i < result["data"].length; i++) {
                names.push(result["data"][i]["name"]);  
            }
            for (var i = 0; i < result["data"].length; i++) {
                nums.push(result["data"][i]["num"]);  
            }
            myChart_2.hideLoading();  
            myChart_2.setOption({ 
                title: {
                    text: 'devx'
                },
                xAxis: {
                    data: names
                },
                series: [{
                    name: 'nums',
                    data: nums
                }]
            });
        }

    },
    error: function(errorMsg) {
        console.log("request failed!");
        myChart_2.hideLoading();
    }
}
window.setInterval(function() {
    $.ajax(getting_2)
}, 1000);












var getting_3 = {
    type: "get",
    url: "http://127.0.0.1:5000/echarts-1",  
    dataType: "json",  
    success: function(result) {
        if (result["type"] === 'bandh') {
            var names = []; 
            var nums = [];   
            for (var i = 0; i < result["data"].length; i++) {
                names.push(result["data"][i]["name"]);   
            }
            for (var i = 0; i < result["data"].length; i++) {
                nums.push(result["data"][i]["num"]);  
            }
            myChart_3.hideLoading(); 
            myChart_3.setOption({  
                title: {
                    text: 'bandh'
                },
                xAxis: {
                    data: names
                },
                series: [{
                    name: 'nums',
                    data: nums
                }]
            });
        }

    },
    error: function(errorMsg) {
        console.log("request failed!");
        myChart_3.hideLoading();
    }
}
window.setInterval(function() {
    $.ajax(getting_3)
}, 1000);