(function(global, undefined) {
  'use strict';

  global.ados_results = global.ados_results || null;

  var PLACEMENT_TYPES_FRIENDLY_NAMES = {
    '5': '300x250',
    '8': '300x100',
  };
  var RATE_TYPE_FRIENDLY_NAMES = {
    '1': 'Flat',
    '2': 'CPM',
    '3': 'CPC',
  }
  var NETWORK = global.ADS_GLOBALS.network;
  var SITE = global.ADS_GLOBALS.site;
  var PRIORITIES = global.ADS_GLOBALS.priorities;
  var PRIORITY_FRIEND_NAMES = Object.keys(PRIORITIES).reduce(function(r, key) {
    r[PRIORITIES[key]] = key
    return r;
  }, {});
  var PLACEMENT_TYPES = {
    main: 5,
    sponsorship: 8,
  };
  var ZONES = global.ADS_GLOBALS.zones;

  function asc(a, b) {
    return a > b ? 1 : -1
  }

  function lower(s) {
    return s.toLowerCase();
  }

  function getPriorityName(id) {
    return PRIORITY_FRIEND_NAMES[id] || 'unknown (' + id + ')';
  }

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

  function encodeProperties(props) {
    // Ensure all property values are URI encoded since
    // ados doesn't handle this properly.
    var encoded = {};
    for (var key in properties) {
      encoded[key] = encodeURIComponent(properties[key]);
    }

    return encoded;
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
    var instrumentedProperties = {
      age_hours: properties.age_hours,
      percentage: properties.percentage,
    };

    var requestPayload = {
      keywords: config.keywords.map(lower).sort(asc),
      placements: [],
      properties: instrumentedProperties,
    };

    if (config.placements) {
      var placements = config.placements.split(',');

      for (var i = 0; i < placements.length; i++) {
        var kvp = placements[i].split(':');
        var type = kvp[0];
        var creative = kvp[1];

        placement = ados_add_placement(NETWORK, SITE, type, PLACEMENT_TYPES[type]);
        placement.setFlightCreativeId(creative);
        placement.setProperties(encodeProperties(properties));

        requestPayload.placements.push({
          name: 'sidebar_' + type,
          types: [
            PLACEMENT_TYPES_FRIENDLY_NAMES[PLACEMENT_TYPES[type]],
          ],
        })
      }
    } else {
      for (var type in PLACEMENT_TYPES) {
        var placement = ados_add_placement(NETWORK, SITE, type, PLACEMENT_TYPES[type]);
        if(properties.double_sidebar && type === 'top'){
          if (ZONES['above_the_fold']) {
            placement.setZone(ZONES['above_the_fold']);
          }
          properties.frame_id = 'ad_main_top';
        } else {
          if (ZONES['below_the_fold']) {
            placement.setZone(ZONES['below_the_fold']);
          }
          properties.frame_id = 'ad_main';
        }
        placement.setProperties(encodeProperties(properties));

        requestPayload.placements.push({
          name: 'sidebar_' + type,
          types: [
            PLACEMENT_TYPES_FRIENDLY_NAMES[PLACEMENT_TYPES[type]],
          ],
        })
      }
    }
    
    ados_setWriteResults(true);

    if (config.keywords) {
      ados_setKeywords(config.keywords);
    }

    r.frames.postMessage(global.parent, 'request.adzerk', requestPayload);

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

          var adResult = global.ados_ads[key];
          var impressionMatcher = global.ados_results[key].match(new RegExp(
            '.*https?:\/\/' +
            ados.domain +
            '\/e.gif\?e=([^&]+).*'
          ));
          var responsePayload = {
            keywords: config.keywords.map(lower).sort(asc),
            placement_type: PLACEMENT_TYPES_FRIENDLY_NAMES[adResult.creative.adType],
            placement_name: 'sidebar_' + key,
            adserver_campaign_id: adResult.flight.campaign.id,
            adserver_flight_id: adResult.flight.id,
            adserver_creative_id: adResult.creative.id,
            adserver_ad_id: adResult.id,
            priority: getPriorityName(adResult.flight.priorityId),
            rate_type: RATE_TYPE_FRIENDLY_NAMES[adResult.flight.rateType],
            ecpm: adResult.ecpm,
            companions: (adResult.companions || []).map(function(c) {
              return {
                adserver_ad_id: c.id,
                placement_type: PLACEMENT_TYPES_FRIENDLY_NAMES[c.adType],
              }
            }),
            properties: instrumentedProperties,
          };

          if (impressionMatcher) {
            var impressionb64data = impressionMatcher[1];

            // this is url safe base64, need to fix the padding and replace escape characters
            impressionb64data = impressionb64data + Array((impressionb64data.length % 4) + 1).join('=')
            impressionb64data = impressionb64data
                                  .replace(/\-/g, '+')
                                  .replace(/_/g, '\/');

            try {
              var impressionData = JSON.parse(global.atob(impressionb64data));

              responsePayload.matched_keywords = impressionData.mk.map(lower).sort(asc);
              responsePayload.interana_excluded = responsePayload.interana_excluded || {};
              responsePayload.interana_excluded.impression_id = impressionData.di;
            } catch (e) {
              // pass
            }
          }

          r.frames.postMessage(global.parent, 'response.adzerk', responsePayload);
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
