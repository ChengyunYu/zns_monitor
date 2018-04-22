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

myChart_1.showLoading(); 
myChart_2.showLoading();
myChart_3.showLoading();   

var names_topk = [];  
var nums_topk = []; 
var names_devx = [];  
var nums_devx = []; 
var names_bandh = [];  
var nums_bandh = []; 

var getting = {
    type: "get",
    url: "http://127.0.0.1:5000/echarts-1",  
    dataType: "json",  
    success: function(result) {
        console.log('result:', result);
        if(typeof(result) === 'undefined'){
            console.log('undefined data')
        }else if(result.length === 0){
            console.log('empty')
        }else if(result[0]['type'] === 'topk') {
            console.log('caught topk!!!!!!!!!!')
            names_topk = [];  
            nums_topk = []; 
            for (var i = 0; i < result[0]["data"].length; i++) {
                names_topk.push(result[0]["data"][i]["name"]); 
            }
            for (var i = 0; i < result[0]["data"].length; i++) {
                nums_topk.push(result[0]["data"][i]["num"]); 
            }
            myChart_1.hideLoading(); 
            myChart_1.setOption({  
                title: {
                    text: 'top K'
                },
                xAxis: {
                    data: names_topk
                },
                series: [{
                    name: 'nums',
                    data: nums_topk
                }]
            });
        }else if(result[0]['type'] === 'devx') {
            console.log('caught devx!!!!!!!!!!')
            names_devx = [];  
            nums_devx = []; 
            for (var i = 0; i < result[0]["data"].length; i++) {
                names_devx.push(result[0]["data"][i]["name"]); 
            }
            for (var i = 0; i < result[0]["data"].length; i++) {
                nums_devx.push(result[0]["data"][i]["num"]); 
            }
            myChart_2.hideLoading(); 
            myChart_2.setOption({  
                title: {
                    text: 'devx'
                },
                xAxis: {
                    data: names_devx
                },
                series: [{
                    name: 'nums',
                    data: nums_devx
                }]
            });
        }else if(result[0]['type'] === 'bandh') {
            console.log('caught bandh!!!!!!!!!!')
            names_bandh = [];  
            nums_bandh = []; 
            for (var i = 0; i < result[0]["data"].length; i++) {
                names_bandh.push(result[0]["data"][i]["name"]); 
            }
            for (var i = 0; i < result[0]["data"].length; i++) {
                nums_bandh.push(result[0]["data"][i]["num"]); 
            }
            myChart_3.hideLoading(); 
            myChart_3.setOption({  
                title: {
                    text: 'bandh'
                },
                xAxis: {
                    data: names_bandh
                },
                series: [{
                    name: 'nums',
                    data: nums_bandh
                }]
            });
        }

    },
    error: function(errorMsg) {
        console.log("request fail", 10);
        myChart_1.hideLoading();
    }
}
window.setInterval(function() {
    $.ajax(getting)
}, 500);









