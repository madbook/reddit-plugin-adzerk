(function(global, undefined) {
  'use strict';

  global.ados_results = global.ados_results || null;

  var NETWORK = global.ADS_GLOBALS.network;
  var SITE = global.ADS_GLOBALS.site;
  var PLACEMENT_TYPES = {
    main: 5,
    top: 5,
    sponsorship: 8,
  };

  function getConfig() {
    // Accessing `location.hash` directly does different
    // things in different browsers:
    //    > location.hash = "#%30";
    //    > location.hash === "#0"; // This is wrong, it should be "#%30"
    //    > true 
    // see http://stackoverflow.com/a/1704842/704286
    var hash = location.href.split('#')[1] || '';

    // Firefox automatically encodes thing the fragment, but not other browsers.
    if (/^\{%22/.test(hash)) {
      hash = decodeURIComponent((hash));
    }

    try {
      return $.parseJSON(hash);
    } catch (e) {
      return {};
    }
  }

  var config = getConfig();
  var properties = config.properties || {};

  // If double sidebar experiment, have the top ad not call Adzerk
  if (properties.double_sidebar && properties.frame_id == 'ad_main_top') {
    var PLACEMENT = 'top';
    // Change the id of the ad so ados knows what div to insert to
    $('#main').attr('id', 'top');
    global.onload = function() {
      var adContent = global.parent.frames.ad_main.ados_results;
      global.name = 'ad-' + PLACEMENT;
      // Grabs data from the main frame and `eval`s it. Unfortunately
      // there isn't a great way to pass this data across frames
      // as it's more complicated than json and would normally be
      // loaded as a script tag, hence the `eval`
      eval(adContent[PLACEMENT]);
    };

    ados.run.push(function() {
      ados_loadDiv(PLACEMENT);
    });
    return;
  }

  // Allows the yield manager to target a percentage of users
  // with specific SSPs.
  if (!properties.hasOwnProperty('percentage')) {
    properties.percentage = Math.round(Math.random() * 100);
  }

  // Ensure all property values are URI encoded since
  // ados doesn't handle this properly.
  for (var key in properties) {
    properties[key] = encodeURIComponent(properties[key]);
  }

  // Display a random image in lieu of an ad for certain keywords.
  // This reduces the number of ad requests for low-fill targets.
  if (global.SKIP_AD_PROBABILITY && Math.random() <= global.SKIP_AD_PROBABILITY) {
    var keywords = config.keywords ? config.keywords : [];
    var skipAd = false;

    if (global.SKIP_AD_KEYWORDS && keywords) {
      for (var i = 0; i < keywords.length; i++) {
        if ($.inArray(keywords[i], global.SKIP_AD_KEYWORDS) !== -1) {
          skipAd = true;
          break;
        }
      }
    }

    if (skipAd) {
      var adframe = document.getElementById('main');
      var img = document.createElement('img');
      var randomImgIndex = Math.floor(Math.random() * global.SKIP_AD_IMAGES.length);
      img.height = 250;
      img.width = 300;
      img.src = global.SKIP_AD_IMAGES[randomImgIndex];

      adframe.appendChild(img);

      return;
    }
  }

  // Reconfigure placements if it's the double sidebar experiment
  if (properties.double_sidebar) {
    PLACEMENT_TYPES = {
      main: 5,
      top: 5,
    };
  }

  ados.run.push(function() {
    ados.isAsync = true;
    var placement = null;
    var request = {
      keywords: config.keywords,
      properties: config.properties,
      placement_types: [],
    };

    if (config.placements) {
      var placements = config.placements.split(',');

      for (var i = 0; i < placements.length; i++) {
        var kvp = placements[i].split(':');
        var type = kvp[0];
        var creative = kvp[1];

        placement = ados_add_placement(NETWORK, SITE, type, PLACEMENT_TYPES[type]);
        placement.setFlightCreativeId(creative);
        placement.setProperties(properties);

        request.placement_types.push(PLACEMENT_TYPES[type]);
      }
    } else {
      for (var type in PLACEMENT_TYPES) {
        placement = ados_add_placement(NETWORK, SITE, type, PLACEMENT_TYPES[type]);
        placement.setProperties(properties);

        request.placement_types.push(PLACEMENT_TYPES[type]);
      }
    }
    
    ados_setWriteResults(true);

    if (config.keywords) {
      ados_setKeywords(config.keywords);
    }

    r.frames.postMessage(global.parent, 'request.adzerk', request);

    ados_load();

    var load = setInterval(function() {
      if (global.ados_results) {
        clearInterval(load);

        // Load top ad if exists
        if (global.ados_results.top && global.postMessage) {
          global.parent.postMessage('ados.createAdFrame:top', config.origin);
        }

        for (var key in global.ados_ads) {
          if (!global.ados_ads.hasOwnProperty(key)) {
            continue;
          }

          r.frames.postMessage(global.parent, 'response.adzerk', {
            keywords: request.keywords,
            properties: request.properties,
            placement_types: request.placement_types,
            placement_name: 'banner_' + key,
            campaign_id: global.ados_ads[key].flight.campaign.id,
            flight_id: global.ados_ads[key].flight.id,
            creative_id: global.ados_ads[key].creative.id,
            ad_id: global.ados_ads[key].id,
            priority_id: global.ados_ads[key].flight.priorityId,
            ad_type: global.ados_ads[key].creative.adType,
          });
        }

        // Load companion
        if (global.ados_results.sponsorship) {
          if (global.postMessage) {
            global.parent.postMessage('ados.createAdFrame:sponsorship', config.origin);
          } else {
            iframe = document.createElement('iframe');
            iframe.src = '/static/createadframe.html';
            iframe.style.display = 'none';
            document.documentElement.appendChild(iframe);
          }
        }
      }
    }, 50);
  });
})(this);
