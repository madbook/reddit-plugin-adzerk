import json
import requests
import sys

from pylons import app_globals as g


class AdzerkError(Exception):
    def __init__(self, status_code, response_body):
        message = "(%s) %s" % (status_code, response_body)
        super(AdzerkError, self).__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class NotFound(AdzerkError): pass


def handle_response(response):
    if not (200 <= response.status_code <= 299):
        try:
            text = response.text
        except TypeError:
            # A TypeError can be raised if the encoding is incorrect
            text = ""

        raise AdzerkError(response.status_code, text)
    try:
        return json.loads(response.text)
    except ValueError:
        raise AdzerkError(response.status_code, response.text)


class Stub(object):
    def __init__(self, Id):
        self.Id = Id

    def _to_item(self):
        return {'Id': self.Id}


class Field(object):
    def __init__(self, name, optional=False):
        self.name = name
        self.optional = optional


class FieldSet(object):
    def __init__(self, *fields):
        self.fields = {field.name for field in fields}
        self.essentials = {field.name for field in fields if not field.optional}

    def to_set(self, exclude_optional=True):
        if exclude_optional:
            return self.essentials
        else:
            return self.fields

    def __iter__(self):
        for field_name in self.fields:
            yield field_name


class Base(object):
    _name = ''
    _base_url = 'https://api.adzerk.net/v1'
    _fields = FieldSet()

    @classmethod
    def _headers(cls):
        return {'X-Adzerk-ApiKey': g.secrets['az_selfserve_key'],
                'Content-Type': 'application/x-www-form-urlencoded'}

    def __init__(self, Id, _is_response=False, **attr):
        self.Id = Id
        missing = self._fields.to_set() - set(attr.keys())
        if missing:
            missing = ', '.join(missing)
            msg = 'missing required attributes: %s' % missing
            if _is_response:
                sys.stderr.write('WARNING: %s' % msg)
            else:
                raise ValueError(msg)

        for attr, val in attr.iteritems():
            self.__setattr__(attr, val, fail_on_unrecognized=(not _is_response))

    def __setattr__(self, attr, val, fail_on_unrecognized=True):
        if attr not in self._fields and attr != 'Id':
            msg = 'unrecognized attribute: %s' % attr
            if fail_on_unrecognized:
                raise ValueError(msg)
            else:
                pass
        object.__setattr__(self, attr, val)

    @classmethod
    def _from_item(cls, item):
        Id = item.pop('Id')
        thing = cls(Id, _is_response=True, **item)
        return thing

    def _to_item(self):
        item = {}
        if self.Id:
            item['Id'] = self.Id
        for attr in self._fields:
            if hasattr(self, attr):
                item[attr] = getattr(self, attr)
        return item

    def _to_data(self):
        return {self._name: json.dumps(self._to_item())}

    @classmethod
    def list(cls, params=None):
        url = '/'.join([cls._base_url, cls._name])
        response = requests.get(url, headers=cls._headers(), params=params)
        content = handle_response(response)
        items = content.get('items')
        if items:
            return [cls._from_item(item) for item in items]

    @classmethod
    def create(cls, **attr):
        url = '/'.join([cls._base_url, cls._name])
        thing = cls(None, **attr)
        data = thing._to_data()
        response = requests.post(url, headers=cls._headers(), data=data)
        item = handle_response(response)
        if isinstance(item.get('Id'), int) and item.get('Id') < 5000:
            g.log.info('item with weird Id: %s' % response.text)
        return cls._from_item(item)

    def _send(self):
        url = '/'.join([self._base_url, self._name, str(self.Id)])
        data = self._to_data()
        response = requests.put(url, headers=self._headers(), data=data)
        item = handle_response(response)

    @classmethod
    def get(cls, Id):
        url = '/'.join([cls._base_url, cls._name, str(Id)])
        response = requests.get(url, headers=cls._headers())
        item = handle_response(response)
        return cls._from_item(item)


class Map(Base):
    parent = None
    parent_id_attr = 'ParentId'
    child = None

    @classmethod
    def list(cls, ParentId):
        url = '/'.join([cls._base_url, cls.parent._name, str(ParentId),
                        cls.child._name + 's'])
        response = requests.get(url, headers=cls._headers())
        content = handle_response(response)
        items = content.get('items')
        if items:
            return [cls._from_item(item) for item in items]

    @classmethod
    def create(cls, ParentId, **attr):
        url = '/'.join([cls._base_url, cls.parent._name, str(ParentId),
                        cls.child._name])
        thing = cls(None, **attr)
        data = thing._to_data()
        response = requests.post(url, headers=cls._headers(), data=data)
        item = handle_response(response)
        return cls._from_item(item)

    def _send(self):
        url = '/'.join([self._base_url, self.parent._name,
                        str(getattr(self, self.parent_id_attr)),
                        self.child._name, str(self.Id)])
        data = self._to_data()
        response = requests.put(url, headers=self._headers(), data=data)
        item = handle_response(response)

    @classmethod
    def get(cls, ParentId, Id):
        url = '/'.join([cls._base_url, cls.parent._name, str(ParentId),
                        cls.child._name, str(Id)])
        response = requests.get(url, headers=cls._headers())
        item = handle_response(response)
        return cls._from_item(item)


class Site(Base):
    _name = 'site'
    _fields = FieldSet(
        Field('Url'),
        Field('Title'),
        Field('PublisherAccountId', optional=True),
        Field('IsDeleted'),
    )

    def __repr__(self):
        return '<Site %s <%s-%s>>' % (self.Id, self.Title, self.Url)


class Zone(Base):
    _name = 'zone'
    _fields = FieldSet(
        Field('Name'),
        Field('SiteId'),
    )

    def __repr__(self):
        return '<Zone %s <%s on Site %s>>' % (self.Id, self.Name, self.SiteId)


class Advertiser(Base):
    _name = 'advertiser'
    _fields = FieldSet(
        Field('Title'),
        Field('IsActive', optional=True),
        Field('IsDeleted', optional=True),
    )

    @classmethod
    def search(cls, Title):
        raise NotImplementedError

    def __repr__(self):
        return '<Advertiser %s <%s>>' % (self.Id, self.Title)


class Flight(Base):
    _name = 'flight'
    _fields = FieldSet(
        Field('Name'),
        Field('StartDate'),
        Field('EndDate', optional=True),
        Field('NoEndDate', optional=True),
        Field('Price'),
        Field('OptionType'),
        Field('Impressions', optional=True),
        Field('IsUnlimited'),
        Field('IsNoDuplicates', optional=True),
        Field('IsFullSpeed'),
        Field('Keywords', optional=True),
        Field('UserAgentKeywords', optional=True),
        Field('CampaignId'),
        Field('PriorityId'),
        Field('IsDeleted'),
        Field('IsActive'),
        Field('GoalType', optional=True),
        Field('RateType', optional=True),
        Field('IsFreqCap', optional=True),
        Field('FreqCap', optional=True),
        Field('FreqCapDuration', optional=True),
        Field('FreqCapType', optional=True),
        Field('DatePartingStartTime', optional=True),
        Field('DatePartingEndTime', optional=True),
        Field('IsSunday', optional=True),
        Field('IsMonday', optional=True),
        Field('IsTuesday', optional=True),
        Field('IsWednesday', optional=True),
        Field('IsThursday', optional=True),
        Field('IsFriday', optional=True),
        Field('IsSaturday', optional=True),
        Field('IPTargeting', optional=True),
        Field('GeoTargeting', optional=True),
        Field('SiteZoneTargeting', optional=True),
        Field('CreativeMaps', optional=True),
        Field('ReferrerKeywords', optional=True),
        Field('WeightOverride', optional=True),
        Field('DeliveryStatus', optional=True),
        Field('CustomTargeting', optional=True),
        Field('DailyCapAmount', optional=True),
        Field('LifetimeCapAmount', optional=True),
        Field('CapType', optional=True),
        Field('BehavioralTargeting', optional=True),
        Field('IsTrackingConversions', optional=True),
    )

    @classmethod
    def _from_item(cls, item):
        if not 'Name' in item:
            item['Name'] = ''   # not always included in response
        if not 'CreativeMaps' in item or not item['CreativeMaps']:
            item['CreativeMaps'] = []
        thing = super(cls, cls)._from_item(item)
        if hasattr(thing, 'CreativeMaps'):
            thing.CreativeMaps = [CreativeFlightMap._from_item(item)
                             for item in thing.CreativeMaps]
        return thing

    @classmethod
    def list(cls, is_active=False):
        return super(Flight, cls).list({"isActive" : is_active})

    def _to_item(self):
        item = Base._to_item(self)
        cfm_things = item.get('CreativeMaps')
        if cfm_things:
            item['CreativeMaps'] = [thing._to_item() for thing in cfm_things]
        return item

    def __repr__(self):
        return '<Flight %s <Campaign %s>>' % (self.Id, self.CampaignId)


class Priority(Base):
    _name = 'priority'
    _fields = FieldSet(
        Field('Name'),
        Field('ChannelId'),
        Field('Weight'),
        Field('IsDeleted'),
    )

    def __repr__(self):
        return '<Priority %s <Weight %s - Channel %s>>' % (self.Id, self.Weight,
                                                           self.ChannelId)


class Creative(Base):
    _name = 'creative'
    _fields = FieldSet(
        Field('Title'),
        Field('Body'),
        Field('Url', optional=True),
        Field('AdvertiserId'),
        Field('AdTypeId'),
        Field('ImageName', optional=True),
        Field('Alt'),
        Field('IsHTMLJS', optional=True),
        Field('ScriptBody', optional=True),
        Field('Metadata', optional=True),
        Field('IsSync'),
        Field('IsDeleted'),
        Field('IsActive'),
        Field('IsNoTrack', optional=True),
    )

    @classmethod
    def list(cls, AdvertiserId):
        url = '/'.join([cls._base_url, 'advertiser', str(AdvertiserId),
                        'creatives'])
        response = requests.get(url, headers=cls._headers())
        content = handle_response(response)
        items = content.get('items')
        if items:
            return [cls._from_item(item) for item in items]

    def __repr__(self):
        return '<Creative %s>' % (self.Id)


class CreativeFlightMap(Map):
    parent = Flight
    parent_id_attr = 'FlightId'
    child = Creative

    _name = 'creative'
    _fields = FieldSet(
        Field('SizeOverride'),
        Field('CampaignId'),
        Field('IsDeleted'),
        Field('Percentage'),
        Field('Iframe'),
        Field('Creative'),
        Field('IsActive'),
        Field('FlightId'),
        Field('Impressions'),
        Field('SiteId', optional=True),
        Field('ZoneId', optional=True),
        Field('DistributionType'),
    )

    def __setattr__(self, attr, val, **kw):
        if attr == 'Creative':
            # Creative could be a full object or just a stub
            d = val
            Id = d.pop('Id')
            if d:
                # if we are not fail_on_unrecognized, assume this is a response
                is_response = not kw.get('fail_on_unrecognized', True)
                val = Creative(Id, _is_response=is_response, **d)
            else:
                val = Stub(Id)
        Map.__setattr__(self, attr, val, **kw)

    @classmethod
    def _from_item(cls, item):
        if not 'SizeOverride' in item:
            item['SizeOverride'] = False    # not always included in response
        if not 'Iframe' in item:
            item['Iframe'] = False  # not always included in response
        thing = super(cls, cls)._from_item(item)
        return thing

    def _to_item(self):
        item = Base._to_item(self)
        item['Creative'] = item['Creative']._to_item()
        return item

    def __repr__(self):
        return '<CreativeFlightMap %s <Creative %s - Flight %s>>' % (
            self.Id,
            self.Creative.Id,
            self.FlightId,
        )


class Channel(Base):
    _name = 'channel'
    _fields = FieldSet(
        Field('Title'),
        Field('Commission'), 
        Field('Engine'), 
        Field('Keywords'), 
        Field('CPM'), 
        Field('AdTypes'),
        Field('IsDeleted'),
    )

    def __repr__(self):
        return '<Channel %s>' % (self.Id)


class Publisher(Base):
    _name = 'publisher'
    _fields = FieldSet(
        Field('FirstName', optional=True),
        Field('LastName', optional=True),
        Field('CompanyName', optional=True),
        Field('PaypalEmail', optional=True),
        Field('PaymentOption', optional=True),
        Field('Address', optional=True),
        Field('IsDeleted'),
    )

    def __repr__(self):
        return '<Publisher %s>' % (self.Id)


class Campaign(Base):
    _name = 'campaign'
    _fields = FieldSet(
        Field('Name'),
        Field('AdvertiserId'),
        Field('SalespersonId'),
        Field('Flights'),
        Field('StartDate'),
        Field('EndDate', optional=True),
        Field('IsDeleted'),
        Field('IsActive'),
        Field('Price'),
    )

    @classmethod
    def get(cls, Id, exclude_flights=False):
        url = '/'.join([cls._base_url, cls._name, str(Id)])
        url += '?excludeFlights=%s' % str(exclude_flights).lower()
        response = requests.get(url, headers=cls._headers())
        item = handle_response(response)
        return cls._from_item(item)

    @classmethod
    def _from_item(cls, item):
        if not 'Flights' in item or not item['Flights']:
            item['Flights'] = []   # not always included in response
        thing = super(cls, cls)._from_item(item)
        if hasattr(thing, 'Flights'):
            thing.Flights = [Flight._from_item(flight)
                             for flight in thing.Flights]
        return thing

    def _to_item(self):
        item = Base._to_item(self)
        flights = item.get('Flights')
        if flights:
            item['Flights'] = [flight._to_item() for flight in flights]
        return item

    def __repr__(self):
        return '<Campaign %s>' % (self.Id)


class GeoTargeting(Base):
    _name = 'geotargeting'
    _fields = FieldSet(
        Field('CountryCode'),
        Field('Region'),
        Field('MetroCode'),
        Field('IsExclude'), # geotargets can include or exclude locations
    )

    @classmethod
    def _from_item(cls, item):
        Id = item.pop('LocationId')
        thing = cls(Id, _is_response=True, **item)
        return thing

    def _send(self, FlightId):
        url = '/'.join([self._base_url, 'flight', str(FlightId), self._name,
                        str(self.Id)])
        data = self._to_data()
        response = requests.put(url, headers=self._headers(), data=data)
        item = handle_response(response)

    def _delete(self, FlightId):
        url = '/'.join([self._base_url, 'flight', str(FlightId), self._name,
                        str(self.Id), 'delete'])
        response = requests.get(url, headers=self._headers())
        message = handle_response(response)

    def __repr__(self):
        return '<GeoTargeting %s>' % (self.Id)
