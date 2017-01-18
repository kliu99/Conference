#!/usr/bin/env python

"""
conference.py -- Udacity conference server-side Python App Engine API;
    uses Google Cloud Endpoints

$Id: conference.py,v 1.25 2014/05/24 23:42:19 wesc Exp wesc $

created by wesc on 2014 apr 21

"""

__author__ = 'wesc+api@google.com (Wesley Chun)'


from datetime import datetime

import endpoints
from protorpc import messages
from protorpc import message_types
from protorpc import remote

from google.appengine.api import memcache
from google.appengine.api import taskqueue
from google.appengine.ext import ndb

from models import ConflictException
from models import Profile
from models import ProfileMiniForm
from models import ProfileForm
from models import StringMessage
from models import BooleanMessage
from models import Conference
from models import ConferenceForm
from models import ConferenceForms
from models import ConferenceQueryForm
from models import ConferenceQueryForms
from models import TeeShirtSize
from models import Session
from models import SessionForm
from models import SessionForms

from settings import WEB_CLIENT_ID
from settings import ANDROID_CLIENT_ID
from settings import IOS_CLIENT_ID
from settings import ANDROID_AUDIENCE

from utils import getUserId

EMAIL_SCOPE = endpoints.EMAIL_SCOPE
API_EXPLORER_CLIENT_ID = endpoints.API_EXPLORER_CLIENT_ID
MEMCACHE_ANNOUNCEMENTS_KEY = "RECENT_ANNOUNCEMENTS"
ANNOUNCEMENT_TPL = ('Last chance to attend! The following conferences '
                    'are nearly sold out: %s')
MEMCACHE_SPEAKER_KEY = "FEATURED_SPEAKERS"
SPEAKER_TPL = ('Featured speaker: %s has the following sessions: %s')
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

DEFAULTS = {
    "city": "Default City",
    "maxAttendees": 0,
    "seatsAvailable": 0,
    "topics": [ "Default", "Topic" ],
}

OPERATORS = {
            'EQ':   '=',
            'GT':   '>',
            'GTEQ': '>=',
            'LT':   '<',
            'LTEQ': '<=',
            'NE':   '!='
            }

FIELDS =    {
            'CITY': 'city',
            'TOPIC': 'topics',
            'MONTH': 'month',
            'MAX_ATTENDEES': 'maxAttendees',
            }

CONF_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    websafeConferenceKey=messages.StringField(1),
)

CONF_POST_REQUEST = endpoints.ResourceContainer(
    ConferenceForm,
    websafeConferenceKey=messages.StringField(1),
)

CONF_SESS_POST_REQUEST = endpoints.ResourceContainer(
    SessionForm,
    websafeConferenceKey=messages.StringField(1),
)

SESS_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    websafeSessionKey=messages.StringField(1),
)

SESS_POST_REQUEST = endpoints.ResourceContainer(
    SessionForm,
    websafeSessionKey=messages.StringField(1),
)

SESS_BY_TYPE_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    websafeConferenceKey=messages.StringField(1),
    typeOfSession=messages.StringField(2)
)

SESS_BY_SPEAKER_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    speaker=messages.StringField(1)
)

SESS_QUERY_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    websafeConferenceKey=messages.StringField(1),
    startDate=messages.StringField(2),
    endDate=messages.StringField(3)
)

SESS_DEFAULTS = {
    "duration": 120,
    "typeOfSession": [ "Default", "Session" ],
    "speaker": "speaker"
}
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


@endpoints.api(name='conference', version='v1', audiences=[ANDROID_AUDIENCE],
    allowed_client_ids=[WEB_CLIENT_ID, API_EXPLORER_CLIENT_ID, ANDROID_CLIENT_ID, IOS_CLIENT_ID],
    scopes=[EMAIL_SCOPE])
class ConferenceApi(remote.Service):
    """Conference API v0.1"""

# - - - Conference objects - - - - - - - - - - - - - - - - -

    def _copyConferenceToForm(self, conf, displayName):
        """Copy relevant fields from Conference to ConferenceForm."""
        cf = ConferenceForm()
        for field in cf.all_fields():
            if hasattr(conf, field.name):
                # convert Date to date string; just copy others
                if field.name.endswith('Date'):
                    setattr(cf, field.name, str(getattr(conf, field.name)))
                else:
                    setattr(cf, field.name, getattr(conf, field.name))
            elif field.name == "websafeKey":
                setattr(cf, field.name, conf.key.urlsafe())
        if displayName:
            setattr(cf, 'organizerDisplayName', displayName)
        cf.check_initialized()
        return cf


    def _createConferenceObject(self, request):
        """Create or update Conference object, returning ConferenceForm/request."""
        # preload necessary data items
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        user_id = getUserId(user)

        if not request.name:
            raise endpoints.BadRequestException("Conference 'name' field required")

        # copy ConferenceForm/ProtoRPC Message into dict
        data = {field.name: getattr(request, field.name) for field in request.all_fields()}
        del data['websafeKey']
        del data['organizerDisplayName']

        # add default values for those missing (both data model & outbound Message)
        for df in DEFAULTS:
            if data[df] in (None, []):
                data[df] = DEFAULTS[df]
                setattr(request, df, DEFAULTS[df])

        # convert dates from strings to Date objects; set month based on start_date
        if data['startDate']:
            data['startDate'] = datetime.strptime(data['startDate'][:10], "%Y-%m-%d").date()
            data['month'] = data['startDate'].month
        else:
            data['month'] = 0
        if data['endDate']:
            data['endDate'] = datetime.strptime(data['endDate'][:10], "%Y-%m-%d").date()

        # set seatsAvailable to be same as maxAttendees on creation
        if data["maxAttendees"] > 0:
            data["seatsAvailable"] = data["maxAttendees"]
        # generate Profile Key based on user ID and Conference
        # ID based on Profile key get Conference key from ID
        p_key = ndb.Key(Profile, user_id)
        c_id = Conference.allocate_ids(size=1, parent=p_key)[0]
        c_key = ndb.Key(Conference, c_id, parent=p_key)
        data['key'] = c_key
        data['organizerUserId'] = request.organizerUserId = user_id

        # create Conference, send email to organizer confirming
        # creation of Conference & return (modified) ConferenceForm
        Conference(**data).put()
        taskqueue.add(params={'email': user.email(),
            'conferenceInfo': repr(request)},
            url='/tasks/send_confirmation_email'
        )
        return request


    @ndb.transactional()
    def _updateConferenceObject(self, request):
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        user_id = getUserId(user)

        # copy ConferenceForm/ProtoRPC Message into dict
        data = {field.name: getattr(request, field.name) for field in request.all_fields()}

        # update existing conference
        conf = ndb.Key(urlsafe=request.websafeConferenceKey).get()
        # check that conference exists
        if not conf:
            raise endpoints.NotFoundException(
                'No conference found with key: %s' % request.websafeConferenceKey)

        # check that user is owner
        if user_id != conf.organizerUserId:
            raise endpoints.ForbiddenException(
                'Only the owner can update the conference.')

        # Not getting all the fields, so don't create a new object; just
        # copy relevant fields from ConferenceForm to Conference object
        for field in request.all_fields():
            data = getattr(request, field.name)
            # only copy fields where we get data
            if data not in (None, []):
                # special handling for dates (convert string to Date)
                if field.name in ('startDate', 'endDate'):
                    data = datetime.strptime(data, "%Y-%m-%d").date()
                    if field.name == 'startDate':
                        conf.month = data.month
                # write to Conference object
                setattr(conf, field.name, data)
        conf.put()
        prof = ndb.Key(Profile, user_id).get()
        return self._copyConferenceToForm(conf, getattr(prof, 'displayName'))


    @endpoints.method(ConferenceForm, ConferenceForm, path='conference',
            http_method='POST', name='createConference')
    def createConference(self, request):
        """Create new conference."""
        return self._createConferenceObject(request)


    @endpoints.method(CONF_POST_REQUEST, ConferenceForm,
            path='conference/{websafeConferenceKey}',
            http_method='PUT', name='updateConference')
    def updateConference(self, request):
        """Update conference w/provided fields & return w/updated info."""
        return self._updateConferenceObject(request)


    @endpoints.method(CONF_GET_REQUEST, ConferenceForm,
            path='conference/{websafeConferenceKey}',
            http_method='GET', name='getConference')
    def getConference(self, request):
        """Return requested conference (by websafeConferenceKey)."""
        # get Conference object from request; bail if not found
        conf = ndb.Key(urlsafe=request.websafeConferenceKey).get()
        if not conf:
            raise endpoints.NotFoundException(
                'No conference found with key: %s' % request.websafeConferenceKey)
        prof = conf.key.parent().get()
        # return ConferenceForm
        return self._copyConferenceToForm(conf, getattr(prof, 'displayName'))


    @endpoints.method(message_types.VoidMessage, ConferenceForms,
            path='getConferencesCreated',
            http_method='POST', name='getConferencesCreated')
    def getConferencesCreated(self, request):
        """Return conferences created by user."""
        # make sure user is authed
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        user_id = getUserId(user)

        # create ancestor query for all key matches for this user
        confs = Conference.query(ancestor=ndb.Key(Profile, user_id))
        prof = ndb.Key(Profile, user_id).get()
        # return set of ConferenceForm objects per Conference
        return ConferenceForms(
            items=[self._copyConferenceToForm(conf, getattr(prof, 'displayName')) for conf in confs]
        )


    def _getQuery(self, request):
        """Return formatted query from the submitted filters."""
        q = Conference.query()
        inequality_filter, filters = self._formatFilters(request.filters)

        # If exists, sort on inequality filter first
        if not inequality_filter:
            q = q.order(Conference.name)
        else:
            q = q.order(ndb.GenericProperty(inequality_filter))
            q = q.order(Conference.name)

        for filtr in filters:
            if filtr["field"] in ["month", "maxAttendees"]:
                filtr["value"] = int(filtr["value"])
            formatted_query = ndb.query.FilterNode(filtr["field"], filtr["operator"], filtr["value"])
            q = q.filter(formatted_query)
        return q


    def _formatFilters(self, filters):
        """Parse, check validity and format user supplied filters."""
        formatted_filters = []
        inequality_field = None

        for f in filters:
            filtr = {field.name: getattr(f, field.name) for field in f.all_fields()}

            try:
                filtr["field"] = FIELDS[filtr["field"]]
                filtr["operator"] = OPERATORS[filtr["operator"]]
            except KeyError:
                raise endpoints.BadRequestException("Filter contains invalid field or operator.")

            # Every operation except "=" is an inequality
            if filtr["operator"] != "=":
                # check if inequality operation has been used in previous filters
                # disallow the filter if inequality was performed on a different field before
                # track the field on which the inequality operation is performed
                if inequality_field and inequality_field != filtr["field"]:
                    raise endpoints.BadRequestException("Inequality filter is allowed on only one field.")
                else:
                    inequality_field = filtr["field"]

            formatted_filters.append(filtr)
        return (inequality_field, formatted_filters)


    @endpoints.method(ConferenceQueryForms, ConferenceForms,
            path='queryConferences',
            http_method='POST',
            name='queryConferences')
    def queryConferences(self, request):
        """Query for conferences."""
        conferences = self._getQuery(request)

        # need to fetch organiser displayName from profiles
        # get all keys and use get_multi for speed
        organisers = [(ndb.Key(Profile, conf.organizerUserId)) for conf in conferences]
        profiles = ndb.get_multi(organisers)

        # put display names in a dict for easier fetching
        names = {}
        for profile in profiles:
            names[profile.key.id()] = profile.displayName

        # return individual ConferenceForm object per Conference
        return ConferenceForms(
                items=[self._copyConferenceToForm(conf, names[conf.organizerUserId]) for conf in \
                conferences]
        )


# - - - Profile objects - - - - - - - - - - - - - - - - - - -

    def _copyProfileToForm(self, prof):
        """Copy relevant fields from Profile to ProfileForm."""
        # copy relevant fields from Profile to ProfileForm
        pf = ProfileForm()
        for field in pf.all_fields():
            if hasattr(prof, field.name):
                # convert t-shirt string to Enum; just copy others
                if field.name == 'teeShirtSize':
                    setattr(pf, field.name, getattr(TeeShirtSize, getattr(prof, field.name)))
                else:
                    setattr(pf, field.name, getattr(prof, field.name))
        pf.check_initialized()
        return pf


    def _getProfileFromUser(self):
        """Return user Profile from datastore, creating new one if non-existent."""
        # make sure user is authed
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')

        # get Profile from datastore
        user_id = getUserId(user)
        p_key = ndb.Key(Profile, user_id)
        profile = p_key.get()
        # create new Profile if not there
        if not profile:
            profile = Profile(
                key = p_key,
                displayName = user.nickname(),
                mainEmail= user.email(),
                teeShirtSize = str(TeeShirtSize.NOT_SPECIFIED),
            )
            profile.put()

        return profile      # return Profile


    def _doProfile(self, save_request=None):
        """Get user Profile and return to user, possibly updating it first."""
        # get user Profile
        prof = self._getProfileFromUser()

        # if saveProfile(), process user-modifyable fields
        if save_request:
            for field in ('displayName', 'teeShirtSize'):
                if hasattr(save_request, field):
                    val = getattr(save_request, field)
                    if val:
                        setattr(prof, field, str(val))
                        #if field == 'teeShirtSize':
                        #    setattr(prof, field, str(val).upper())
                        #else:
                        #    setattr(prof, field, val)
                        prof.put()

        # return ProfileForm
        return self._copyProfileToForm(prof)


    @endpoints.method(message_types.VoidMessage, ProfileForm,
            path='profile', http_method='GET', name='getProfile')
    def getProfile(self, request):
        """Return user profile."""
        return self._doProfile()


    @endpoints.method(ProfileMiniForm, ProfileForm,
            path='profile', http_method='POST', name='saveProfile')
    def saveProfile(self, request):
        """Update & return user profile."""
        return self._doProfile(request)


# - - - Announcements - - - - - - - - - - - - - - - - - - - -

    @staticmethod
    def _cacheAnnouncement():
        """Create Announcement & assign to memcache; used by
        memcache cron job & putAnnouncement().
        """
        confs = Conference.query(ndb.AND(
            Conference.seatsAvailable <= 5,
            Conference.seatsAvailable > 0)
        ).fetch(projection=[Conference.name])

        if confs:
            # If there are almost sold out conferences,
            # format announcement and set it in memcache
            announcement = ANNOUNCEMENT_TPL % (
                ', '.join(conf.name for conf in confs))
            memcache.set(MEMCACHE_ANNOUNCEMENTS_KEY, announcement)
        else:
            # If there are no sold out conferences,
            # delete the memcache announcements entry
            announcement = ""
            memcache.delete(MEMCACHE_ANNOUNCEMENTS_KEY)

        return announcement


    @endpoints.method(message_types.VoidMessage, StringMessage,
            path='conference/announcement/get',
            http_method='GET', name='getAnnouncement')
    def getAnnouncement(self, request):
        """Return Announcement from memcache."""
        return StringMessage(data=memcache.get(MEMCACHE_ANNOUNCEMENTS_KEY) or "")


# - - - Registration - - - - - - - - - - - - - - - - - - - -

    @ndb.transactional(xg=True)
    def _conferenceRegistration(self, request, reg=True):
        """Register or unregister user for selected conference."""
        retval = None
        prof = self._getProfileFromUser() # get user Profile

        # check if conf exists given websafeConfKey
        # get conference; check that it exists
        wsck = request.websafeConferenceKey
        conf = ndb.Key(urlsafe=wsck).get()
        if not conf:
            raise endpoints.NotFoundException(
                'No conference found with key: %s' % wsck)

        # register
        if reg:
            # check if user already registered otherwise add
            if wsck in prof.conferenceKeysToAttend:
                raise ConflictException(
                    "You have already registered for this conference")

            # check if seats avail
            if conf.seatsAvailable <= 0:
                raise ConflictException(
                    "There are no seats available.")

            # register user, take away one seat
            prof.conferenceKeysToAttend.append(wsck)
            conf.seatsAvailable -= 1
            retval = True

        # unregister
        else:
            # check if user already registered
            if wsck in prof.conferenceKeysToAttend:

                # unregister user, add back one seat
                prof.conferenceKeysToAttend.remove(wsck)
                conf.seatsAvailable += 1
                retval = True
            else:
                retval = False

        # write things back to the datastore & return
        prof.put()
        conf.put()
        return BooleanMessage(data=retval)


    @endpoints.method(message_types.VoidMessage, ConferenceForms,
            path='conferences/attending',
            http_method='GET', name='getConferencesToAttend')
    def getConferencesToAttend(self, request):
        """Get list of conferences that user has registered for."""
        prof = self._getProfileFromUser() # get user Profile
        conf_keys = [ndb.Key(urlsafe=wsck) for wsck in prof.conferenceKeysToAttend]
        conferences = ndb.get_multi(conf_keys)

        # get organizers
        organisers = [ndb.Key(Profile, conf.organizerUserId) for conf in conferences]
        profiles = ndb.get_multi(organisers)

        # put display names in a dict for easier fetching
        names = {}
        for profile in profiles:
            names[profile.key.id()] = profile.displayName

        # return set of ConferenceForm objects per Conference
        return ConferenceForms(items=[self._copyConferenceToForm(conf, names[conf.organizerUserId])\
         for conf in conferences]
        )


    @endpoints.method(CONF_GET_REQUEST, BooleanMessage,
            path='conference/{websafeConferenceKey}',
            http_method='POST', name='registerForConference')
    def registerForConference(self, request):
        """Register user for selected conference."""
        return self._conferenceRegistration(request)


    @endpoints.method(CONF_GET_REQUEST, BooleanMessage,
            path='conference/{websafeConferenceKey}',
            http_method='DELETE', name='unregisterFromConference')
    def unregisterFromConference(self, request):
        """Unregister user for selected conference."""
        return self._conferenceRegistration(request, reg=False)


    @endpoints.method(message_types.VoidMessage, ConferenceForms,
            path='filterPlayground',
            http_method='GET', name='filterPlayground')
    def filterPlayground(self, request):
        """Filter Playground"""
        q = Conference.query()
        # field = "city"
        # operator = "="
        # value = "London"
        # f = ndb.query.FilterNode(field, operator, value)
        # q = q.filter(f)
        q = q.filter(Conference.city=="London")
        q = q.filter(Conference.topics=="Medical Innovations")
        q = q.filter(Conference.month==6)

        return ConferenceForms(
            items=[self._copyConferenceToForm(conf, "") for conf in q]
        )

# - - - Session objects - - - - - - - - - - - - - - - - -
    def _copySessionToForm(self, sess):
        """Copy relevant fields from Conference to ConferenceForm."""
        se = SessionForm()
        for field in se.all_fields():
            if hasattr(sess, field.name):
                # convert Date to date string; just copy others
                if field.name == 'date':
                    setattr(se, field.name, str(getattr(sess, field.name)))
                elif field.name == 'startTime':
                    setattr(se, field.name, str(getattr(sess, field.name)))
                else:
                    setattr(se, field.name, getattr(sess, field.name))

        se.check_initialized()
        return se


    def _createSessionObject(self, request):
        """Create or update Conference object, returning ConferenceForm/request."""
        # preload necessary data items
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        user_id = getUserId(user)

        conf_key = ndb.Key(urlsafe=request.websafeConferenceKey)
        conf = conf_key.get()

        if not (conf and conf_key.kind() == 'Conference'):
            raise endpoints.NotFoundException(
                'No conference found with key: %s' % request.websafeConferenceKey)

        # Organizer of the conference
        prof = conf.key.parent().get()

        if user_id != prof.key.id():
            raise endpoints.UnauthorizedException("You are not the organizer of this conference")

        if not request.name:
            raise endpoints.BadRequestException("Session 'name' field required")


        # copy SessionForm/ProtoRPC Message into dict
        data = {field.name: getattr(request, field.name) for field in request.all_fields()}

        del data['websafeConferenceKey']

        # add default values for those missing (both data model & outbound Message)
        for df in SESS_DEFAULTS:
            if data[df] in (None, []):
                data[df] = SESS_DEFAULTS[df]
                setattr(request, df, SESS_DEFAULTS[df])

        # convert dates from strings to Date objects; set month based on start_date
        if data['date']:
            data['date'] = datetime.strptime(data['date'][:10], "%Y-%m-%d").date()

        if data['startTime']:
            data['startTime'] = datetime.strptime(data['startTime'], "%H:%M").time()

        # generate Profile Key based on user ID and Conference
        # ID based on Profile key get Conference key from ID
        c_key = conf.key
        s_id = Session.allocate_ids(size=1, parent=c_key)[0]
        s_key = ndb.Key(Session, s_id, parent=c_key)
        data['key'] = s_key
        data['organizerUserId'] = request.organizerUserId = user_id

        # create Conference, send email to organizer confirming
        # creation of Conference & return (modified) ConferenceForm
        sess_key = Session(**data).put()
        taskqueue.add(url='/tasks/featured_speaker',
                      params={'speaker': sess_key.get().speaker}
                      )

        return self._copySessionToForm(sess_key.get())

    @endpoints.method(CONF_SESS_POST_REQUEST, SessionForm, path='conference/{websafeConferenceKey}/createSession',
                      http_method='POST', name='createSession')
    def createSession(self, request):
        """Create new session."""
        return self._createSessionObject(request)

    @endpoints.method(CONF_GET_REQUEST, SessionForms,
            path='conference/{websafeConferenceKey}/sessions',
            http_method='GET', name='getConferenceSessions')
    def getConferenceSessions(self, request):
        """Return conference sessions"""

        conf = ndb.Key(urlsafe=request.websafeConferenceKey).get()
        if not conf:
            raise endpoints.NotFoundException(
                'No conference found with key: %s' % request.websafeConferenceKey)

        # create ancestor query for all key matches for this user
        sesss = Session.query(ancestor=conf.key)

        # return set of ConferenceForm objects per Conference
        return SessionForms(
            items=[self._copySessionToForm(sess) for sess in sesss]
        )

    @endpoints.method(SESS_BY_TYPE_GET_REQUEST, SessionForms,
                      path='conference/{websafeConferenceKey}/sessions/{typeOfSession}',
                      http_method='GET', name='getConferenceSessionsByType')
    def getConferenceSessionsByType(self, request):
        """Return conference sessions by Type"""

        conf = ndb.Key(urlsafe=request.websafeConferenceKey).get()
        if not conf:
            raise endpoints.NotFoundException(
                'No conference found with key: %s' % request.websafeConferenceKey)

        # create ancestor query for all key matches for this user
        sesss = Session.query(Session.typeOfSession == request.typeOfSession, ancestor=conf.key)
        # return set of ConferenceForm objects per Conference
        return SessionForms(
            items=[self._copySessionToForm(sess) for sess in sesss]
        )

    @endpoints.method(SESS_BY_SPEAKER_GET_REQUEST, SessionForms,
                      path='sessions/{speaker}',
                      http_method='GET', name='getSessionsBySpeaker')
    def getSessionsBySpeaker(self, request):
        """Return conference sessions by speaker"""

        sesss = Session.query(Session.speaker == request.speaker)

        # return set of ConferenceForm objects per Conference
        return SessionForms(
            items=[self._copySessionToForm(sess) for sess in sesss]
        )

    # Wish list
    def _sessionRegistration(self, request, reg=True):
        """Register or unregister user for selected conference."""
        retval = None
        prof = self._getProfileFromUser()  # get user Profile

        # check if conf exists given websafeConfKey
        # get conference; check that it exists
        wssk = request.websafeSessionKey
        sess_key = ndb.Key(urlsafe=wssk)
        sess = sess_key.get()

        if not (sess and sess_key.kind() == 'Session'):
            raise endpoints.NotFoundException(
                'No session found with key: %s' % wssk)

        # register
        if reg:
            # check if user already registered otherwise add
            if wssk in prof.sessionKeysToAttend:
                raise ConflictException(
                    "You have already registered for this conference")

            # register user, take away one seat
            prof.sessionKeysToAttend.append(wssk)
            retval = True

        # unregister
        else:
            # check if user already registered
            if wssk in prof.sessionKeysToAttend:

                # unregister user, add back one seat
                prof.sessionKeysToAttend.remove(wssk)
                retval = True
            else:
                retval = False

        # write things back to the datastore & return
        prof.put()
        return BooleanMessage(data=retval)

    @endpoints.method(CONF_GET_REQUEST, SessionForms,
            path='session/attending',
            http_method='GET', name='getSessionsInWishlist')
    def getSessionsInWishlist(self, request):
        """Get list of sessions that user is interested in."""
        prof = self._getProfileFromUser()  # get user Profile

        sess_keys = [ndb.Key(urlsafe=wssk) for wssk in prof.sessionKeysToAttend]

        if request.websafeConferenceKey:
            conf_key = ndb.Key(urlsafe=request.websafeConferenceKey)
            conf = conf_key.get()

            if not (conf and conf_key.kind() == 'Conference'):
                raise endpoints.NotFoundException(
                    'No conference found with key: %s' % request.websafeConferenceKey)

            for key in sess_keys:
                if (key.parent() != conf_key):
                    sess_keys.remove(key)

        sessions = ndb.get_multi(sess_keys)

        # return set of ConferenceForm objects per Conference
        return SessionForms(items=[self._copySessionToForm(sess) \
                                   for sess in sessions]
                            )

    @endpoints.method(SESS_GET_REQUEST, BooleanMessage,
            path='session/wishlist/{websafeSessionKey}',
            http_method='POST', name='addSessionToWishlist')
    def addSessionToWishlist(self, request):
        """Add session to user's wish list."""
        return self._sessionRegistration(request)

    @endpoints.method(SESS_GET_REQUEST, BooleanMessage,
            path='session/wishlist/{websafeSessionKey}',
            http_method='DELETE', name='deleteSessionInWishlist')
    def deleteSessionInWishlist(self, request):
        """Remove session from user's wish list."""
        return self._sessionRegistration(request, reg=False)

    # Additional Query methods
    def _updateSessionObject(self, request):
        # Check if user is logged in
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        user_id = getUserId(user)

        # Check if session exist
        sess_key = ndb.Key(urlsafe=request.websafeSessionKey)
        sess = sess_key.get()

        if not (sess and sess_key.kind() == 'Session'):
            raise endpoints.NotFoundException(
                'No session found with key: %s' % request.websafeSessionKey)

        # Check if the user is the organizer of the session
        organizer_uid = sess.organizerUserId
        if user_id != organizer_uid:
            raise endpoints.UnauthorizedException("You are not the organizer of this session")

        # Not getting all the fields, so don't create a new object; just
        # copy relevant fields from SessionForm to Session object
        for field in request.all_fields():
            data = getattr(request, field.name)
            # only copy fields where we get data
            if data not in (None, []):
                # convert dates from strings to Date objects; set month based on start_date
                if field.name == 'date':
                    data = datetime.strptime(data[:10], "%Y-%m-%d").date()

                if field.name == 'startTime':
                    data = datetime.strptime(data, "%H:%M").time()

                # write to Conference object
                setattr(sess, field.name, data)

        sess.put()

        return self._copySessionToForm(sess)

    @endpoints.method(SESS_POST_REQUEST, BooleanMessage, path='session/{websafeSessionKey}',
                      http_method='DELETE', name='deleteSession')
    def deleteSession(self, request):
        """Delete an existing session."""
        # Check if user is logged in
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        user_id = getUserId(user)

        # Check if session exist
        sess_key = ndb.Key(urlsafe=request.websafeSessionKey)
        sess = sess_key.get()

        if not (sess and sess_key.kind() == 'Session'):
            raise endpoints.NotFoundException(
                'No session found with key: %s' % request.websafeSessionKey)

        # Check if the user is the organizer of the session
        organizer_uid = sess.organizerUserId
        if user_id != organizer_uid:
            raise endpoints.UnauthorizedException("You are not the organizer of this session")

        sess_key.delete()

        return BooleanMessage(data=True)

    @endpoints.method(SESS_POST_REQUEST, SessionForm,
                      path='session/{websafeSessionKey}',
                      http_method='PUT', name='updateSession')
    def updateSession(self, request):
        """Update session w/provided fields & return w/updated info."""

        return self._updateSessionObject(request)

    @endpoints.method(SESS_GET_REQUEST, SessionForm,
                      path='session/{websafeSessionKey}',
                      http_method='GET', name='getSession')
    def getSession(self, request):
        """Retrieve a specific session info."""

        sess_key = ndb.Key(urlsafe=request.websafeSessionKey)
        sess = sess_key.get()

        if not (sess and sess_key.kind() == 'Session'):
            raise endpoints.NotFoundException(
                'No session found with key: %s' % request.websafeSessionKey)

        return self._copySessionToForm(sess)

    @endpoints.method(SESS_QUERY_GET_REQUEST, SessionForms,
                      path='sessions',
                      http_method='GET', name='getSessionsInDateRange')
    def getSessionsInDateRange(self, request):
        """Query session within a date range"""

        conf_key = None
        startDate = datetime.min.date()
        endDate = datetime.max.date()
        if request.websafeConferenceKey:
            conf_key = ndb.Key(urlsafe=request.websafeConferenceKey)
            conf = conf_key.get()

            if not (conf and conf_key.kind() == 'Conference'):
                raise endpoints.NotFoundException(
                    'No conference found with key: %s' % request.websafeConferenceKey)

        if request.startDate:
            startDate = datetime.strptime(request.startDate[:10], "%Y-%m-%d").date()

        if request.endDate:
            endDate = datetime.strptime(request.endDate[:10], "%Y-%m-%d").date()


        sessions = Session.query(ndb.AND(Session.date >= startDate, Session.date <= endDate), ancestor=conf_key)\
                          .order(Session.date)

        return SessionForms(
            items=[self._copySessionToForm(sess) for sess in sessions]
        )

    @endpoints.method(message_types.VoidMessage, SessionForms,
            path='queryPlayground',
            http_method='GET', name='queryPlayground')
    def queryPlayground(self, request):
        """Query Playground"""

        # https://cloud.google.com/appengine/docs/python/ndb/queries#repeated_properties

        # startTime < 19:00
        # typeOfSession != "Workshop"

        target = datetime.strptime("19:00", "%H:%M").time()

        #
        # Before normalization
        # Error: ( Only one inequality filter per query is supported. Encountered both startTime and typeOfSession )
        # q = Session.query(ndb.AND( Session.startTime < target, Session.typeOfSession != 'Workshop') )


        #
        # Normalize it
        # Assuming typeOfSession are only in ['Default', 'Session', 'Workshop', 'Tutorial', 'Lecture', 'Keynote']

        #
        # Using rule #1 to expand the != operand
        # q = Session.query(ndb.AND(Session.startTime < target,
        #                           ndb.OR( Session.typeOfSession < 'Workshop', Session.typeOfSession > 'Workshop' )
        #                           ) )

        # q = Session.query(ndb.AND(Session.startTime < target,
        #                           ndb.OR( Session.typeOfSession == 'Default',
        #                                   Session.typeOfSession == 'Session',
        #                                   Session.typeOfSession == 'Tutorial',
        #                                   Session.typeOfSession == 'Lecture',
        #                                   Session.typeOfSession == 'Keynote' )
        #                           ))   # However, due to OR's implementation,
        # a query of this form that is too complex might fail with an exception.
        # You are safer if you normalize these filters so there is (at most) a single OR operation
        # at the top of the expression tree, and a single level of AND operations below that.

        #
        # Using rule #2 on the innermost OR nested within an AND:
        q = Session.query(ndb.OR( ndb.AND(Session.startTime < target, Session.typeOfSession == 'Default'),
                                  ndb.AND(Session.startTime < target, Session.typeOfSession == 'Session'),
                                  ndb.AND(Session.startTime < target, Session.typeOfSession == 'Tutorial'),
                                  ndb.AND(Session.startTime < target, Session.typeOfSession == 'Lecture'),
                                  ndb.AND(Session.startTime < target, Session.typeOfSession == 'Keynote')
                                  ))

        return SessionForms(
            items=[self._copySessionToForm(sess) for sess in q]
        )

# - - - Featured Speakers - - - - - - - - - - - - - - - - -
    @staticmethod
    def _cacheSpeakers(speaker):
        """Create Featured speakers announcement & assign to memcache; used by
        memcache cron job & putSpekaer().
        """

        sesss = Session.query(
            Session.speaker == speaker
        ).fetch(projection=[Session.name])

        # More than one session
        if len(sesss) > 1:
            # If there are almost sold out conferences,
            # format announcement and set it in memcache
            announcement = SPEAKER_TPL % (speaker,
                ', '.join(sess.name for sess in sesss))
            memcache.set(MEMCACHE_SPEAKER_KEY, announcement)
        else:
            # If there are no featured speakers,
            # delete the memcache announcements entry
            announcement = ""
            memcache.delete(MEMCACHE_SPEAKER_KEY)

        return announcement


    @endpoints.method(message_types.VoidMessage, StringMessage,
            path='session/speaker/get',
            http_method='GET', name='getFeaturedSpeaker')
    def getFeaturedSpeaker(self, request):
        """Return Announcement from memcache."""
        return StringMessage(data=memcache.get(MEMCACHE_SPEAKER_KEY) or "")

api = endpoints.api_server([ConferenceApi]) # register API
