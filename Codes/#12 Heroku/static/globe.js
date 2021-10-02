// <!-- Chart code -->

am4core.ready(function() {

// Themes begin
am4core.useTheme(am4themes_spiritedaway);
am4core.useTheme(am4themes_animated);
// Themes end

var chart = am4core.create("chartdiv", am4maps.MapChart);

// Set map definition
chart.geodata = am4geodata_worldLow;

// Set projection
chart.projection = new am4maps.projections.Orthographic();
chart.panBehavior = "rotateLongLat";
chart.deltaLatitude = -20;
chart.padding(20,20,20,20);

// limits vertical rotation
chart.adapter.add("deltaLatitude", function(delatLatitude){
    return am4core.math.fitToRange(delatLatitude, -90, 90);
})

// Create map polygon series
var polygonSeries = chart.series.push(new am4maps.MapPolygonSeries());

// Make map load polygon (like country names) data from GeoJSON
polygonSeries.useGeodata = true;

// Configure series
var polygonTemplate = polygonSeries.mapPolygons.template;
polygonTemplate.tooltipText = "{name}";
polygonTemplate.fill = am4core.color("#d3d6da");
polygonTemplate.stroke = am4core.color("#989DB5");
polygonTemplate.strokeWidth = 0.5;

var graticuleSeries = chart.series.push(new am4maps.GraticuleSeries());
graticuleSeries.mapLines.template.line.stroke = am4core.color("#989DB5");
graticuleSeries.mapLines.template.line.strokeOpacity = 0.08;
graticuleSeries.fitExtent = false;


chart.backgroundSeries.mapPolygons.template.polygon.fillOpacity = 0;
chart.backgroundSeries.mapPolygons.template.polygon.fill = am4core.color("#989DB5");

// Create hover state and set alternative fill color
var hs = polygonTemplate.states.create("hover");
hs.properties.fill = chart.colors.getIndex(0).brighten(-0.5);

let animation;
setTimeout(function(){
  animation = chart.animate({property:"deltaLongitude", to:10000}, 500000);
}, 3000)

chart.seriesContainer.events.on("down", function(){
if(animation){
  animation.stop();
}
})

}); 
// end am4core.ready()
