import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';
import * as spotify from './spotify';
import axios, {AxiosResponse} from 'axios';
import FieldValue = admin.firestore.FieldValue;
import { getEnvironment } from "./environment";
import { URLSearchParams } from 'url';
import DocumentReference = admin.firestore.DocumentReference;
import DocumentSnapshot = admin.firestore.DocumentSnapshot;
const { PubSub } = require('@google-cloud/pubsub');

const supportedSpotifyTimeRanges = ['long_term', 'medium_term', 'short_term'];

admin.initializeApp();

const convertSpotifyTrackToHearTheSpearTrack = (spotifyTrack: any) => {
  return {
    name: spotifyTrack['name'],
    artist: spotifyTrack['album']['artists'][0]['name'],
    album: spotifyTrack['album']['name'],
    art: spotifyTrack['album']['images'].pop()['url'],
    artMedium: spotifyTrack['album']['images'].pop()['url'],
    preview: spotifyTrack['preview_url'],
    link: spotifyTrack['external_urls']['spotify'],
    spotifyUri: spotifyTrack['uri']
  }
};

const purgeListeningHistoryOfUser = async (firebaseAuthUID: string, spotifyTimeRange: string) => {
  // Get the user record from the database.
  const userDoc = await admin.firestore().collection('users').doc(firebaseAuthUID).get();

  const batch = admin.firestore().batch();

  // An array of all the track/artist docs that we edited.
  const editedDocs: Array<DocumentReference> = [];

  if (userDoc.data()![spotifyTimeRange + '-tracks']) {
    for (const trackId of userDoc.data()![spotifyTimeRange + '-tracks']) {
      // Get a reference to the counter document for this track.
      const trackDoc = admin.firestore().collection(spotifyTimeRange + '-tracks').doc(trackId);

      if (!(await trackDoc.get()).exists) {
        console.warn('Expected track document to exist but it was not in the database: ' + trackId);
        continue;
      }
      // Decrement the count by one, since this user's history is being purged.
      batch.update(trackDoc, {
        count: FieldValue.increment(-1)
      });
      editedDocs.push(trackDoc);
    }
  }

  if (userDoc.data()![spotifyTimeRange + '-artists']) {
    for (const artistId of userDoc.data()![spotifyTimeRange + '-artists']) {
      // Get a reference to the counter document for this artist.
      const artistDoc = admin.firestore().collection(spotifyTimeRange + '-artists').doc(artistId);
      // Decrement the count by one, since this user's history is being purged.
      if (!(await artistDoc.get()).exists) {
        console.warn('Expected artist document to exist but it was not in the database: ' + artistId);
        continue;
      }
      batch.update(artistDoc, {
        count: FieldValue.increment(-1)
      });
      editedDocs.push(artistDoc);
    }
  }

  // Remove the tracks and artists from this user's individual record.
  const update: any = {};
  update[spotifyTimeRange + '-artists'] = FieldValue.delete();
  update[spotifyTimeRange + '-tracks'] = FieldValue.delete();
  batch.set(userDoc.ref, update, { merge: true });

  // Submit the database changes to Firebase.
  await batch.commit();


  // Upon initial deployment the lists will be empty and there will be no need to run a transaction.
  // In fact running transaction.getAll on an empty array will result in an error.
  if (editedDocs.length > 0) {
    // Now check to see if any of those docs have a count of zero. If so they can be deleted.
    await admin.firestore().runTransaction(async transaction => {
      const docs: Array<DocumentSnapshot<any>> = await transaction.getAll(...editedDocs); // 100 docs
      // doc.exists just for an abundance of safety. It should never be false though.
      const deleters = docs.filter(doc => doc.exists && doc.data().count < 1)
          .map((doc: DocumentSnapshot) => transaction.delete(doc.ref));

      await Promise.all(deleters);
    });
  }

};

/**
 * Collect the listening history of a Spotify user and stores it in the database.
 * This function assumes that there is a Spotify access token stored for the user and that
 * the token is fresh. If you are unsure whether or not a token is fresh, check the expiration date,
 * and if necessary, invoke {@link renewSpotifyAuthToken}.
 * Purges old listening history automatically.
 * @param firebaseAuthUID self explanatory
 * @param spotifyTimeRange for a list of valid timerange strings see spotify's top tracks endpoint documentation
 */
const collectListeningHistory = async (firebaseAuthUID: string, spotifyTimeRange: string) => {
  /**
   * Returns the Spotify endpoint that holds recent listening history.
   * @param dataType acceptable data types are tracks, and artists.
   * @param limit the number of records to return, the maximum value is fifty.
   */
  const getSpotifyEndpoint = (dataType: string, limit: number = 50) =>
      `https://api.spotify.com/v1/me/top/${dataType}?limit=${limit}&time_range=${spotifyTimeRange}`;

  // Get the user record from the database.
  const userDoc = await admin.firestore().collection('users').doc(firebaseAuthUID).get();

  // Make sure that there is a record on file for the user.
  if (!userDoc.exists) {
    throw new Error('Unable to collect listening history for user: ' + firebaseAuthUID + ' because no database record exists for them.' +
        ' Therefore, there is no Spotify access token on record for them.');
  }

  // Pass Spotify authorization keys to Axios http client.
  const httpClientConfig = {
    headers: {
      // Get the Spotify access code for the user.
      'Authorization': 'Bearer ' + userDoc.data()!['spotifyAccessToken']
    }
  };

  await purgeListeningHistoryOfUser(firebaseAuthUID, spotifyTimeRange)

  // Request the user's listening history from the Spotify Web API.

  const [userFavoriteTracksResponse, userFavoriteArtistsResponse] = await Promise.all([
      axios.get(getSpotifyEndpoint('tracks'), httpClientConfig),
      axios.get(getSpotifyEndpoint('artists'), httpClientConfig),
  ]);

  const userFavoriteTracks = userFavoriteTracksResponse.data['items'];
  const userFavoriteArtists = userFavoriteArtistsResponse.data['items'];

  for (const track of userFavoriteTracks) {
    // Example Spotify Track URI: spotify:track:12345
    const trackId = track['uri'].split(':')[2];
    // A reference to the track counter document.
    // This contains a count of all the current users who frequently listen to this song.
    const trackDocRef = admin.firestore().collection(spotifyTimeRange + '-tracks').doc(trackId);

    await admin.firestore().runTransaction(async (transaction) => {
      const trackDoc = await transaction.get(trackDocRef);
      const update: any = {
        // Increment the count by 1, as we have just discovered another frequent listener.
        count: FieldValue.increment(1),
        ...convertSpotifyTrackToHearTheSpearTrack(track),

        // Include a 7 digit random number in the track document. This number will allow
        // us to pull documents from the database in a completely random order (if desired).
        random: Math.round(Math.random() * Math.pow(10, 7))
      };

      // collectListeningHistory() is invoked by syncAllUsers() many times in quick succession. By
      // truncating the date to the nearest hour we prevent the order in which users are processed
      // from effecting the order in which songs appear on the list.
      const truncatedNow = new Date(Date.now());
      truncatedNow.setMilliseconds(0);
      truncatedNow.setSeconds(0);
      truncatedNow.setMinutes(0);

      // firstAppeared is the time that the track got one listener.
      // Tracks whose listener count goes from 0 -> 1 have their firstAppearedValue reset.
      // When polling the database the returned tracks are ordered first by listener count and
      // then by appearance. This means that the newer tracks are given priority over
      // older tracks, keeping the list fresher.
      if (trackDoc.exists) {
        if (trackDoc.data()!.count) { // Check if count === 0 but this is safer
          update.firstAppeared = truncatedNow.valueOf();
        }
      } else {
        update.firstAppeared = truncatedNow.valueOf();
      }

      transaction.update(trackDocRef, update, { merge: true });
    });
  }

  // Create a new database transaction.
  // This will contain all the changes we are about to make.
  const batch = admin.firestore().batch();

  for (const artist of userFavoriteArtists) {
    // Example Spotify Artist URI: spotify:artist:12345
    const artistId = artist['uri'].split(':')[2];
    // A reference to the counter document.
    // This contains a count of all the current users who frequently listen to this artist.
    const doc = admin.firestore().collection(spotifyTimeRange + '-artists').doc(artistId);
    const update = {
      // Increment the count by 1, as we have just discovered another frequent listener.
      count: FieldValue.increment(1),
      // Store the name of the track, in case this is the first time it has been recorded in our system.
      name: artist['name'],
      // Store a link to the artist's profile in case this is the first the artist has been recorded in our system.
      link: artist['external_urls']['spotify'],
      profilePicture: null
    };

    // Store the artist's profile picture, in case this is the first it has been recorded in our system.
    // The last image in the list is always the smallest, perfect for our icons.
    if (artist['images'].length > 0) {
      update.profilePicture = artist['images'].pop()['url'];
    }

    batch.set(doc, update, { merge: true });
  }

  // Store a list of the all the tracks and artists this user likes.
  // That way, we can decrement their counts if the user decides to disconnect their account or if we
  // get new data from Spotify.
  const userRecordUpdate: any = {};

  userRecordUpdate[spotifyTimeRange + '-tracks']
      // @ts-ignore
      = userFavoriteTracks.map(track => track['uri'].replace('spotify:track:', ''));

  userRecordUpdate[spotifyTimeRange + '-artists']
      // @ts-ignore
      = userFavoriteArtists.map(artist => artist['uri'].replace('spotify:artist:', ''));

  batch.set(userDoc.ref, userRecordUpdate, { merge: true });

  // Submit the database changes to Firebase.
  await batch.commit();
};


/**
 * Spotify 3rd party application auth tokens periodically expire.
 * Use refreshSpotifyAuthToken to request a fresh new auth token.
 * Store the new token in the database.
 * @param uid The Firebase Auth UID that represents the user.
 */
const renewSpotifyAuthToken = async (uid: string) => {
  console.log('Attempting to renew Spotify authorization for user: ' + uid);

  const userData = (await admin.firestore().collection('users').doc(uid).get()).data();
  if (!userData) {
    throw new Error('A database entry must be created for the user before invoking this function.');
    // Not only that, but the database entry must also contain a refreshToken.
  }
  const refreshToken = userData['spotifyRefreshToken'];
  // This will contain all the parameters that Spotify expects us to provide
  // when requesting a fresh auth token.
  const reqParams = new URLSearchParams();

  if (!refreshToken) {
    // If no refresh token is found in the database then the user is considered "Dead".
    // Without a refresh token we can't request a new auth token from Spotify.

    await Promise.all([
      purgeListeningHistoryOfUser(uid, 'short_term'),
      purgeListeningHistoryOfUser(uid, 'medium_term'),
      purgeListeningHistoryOfUser(uid, 'long_term'),
    ]);
    await admin.firestore().collection('users').doc(uid).delete();

    throw new Error('No refresh token found in database. This function is for renewing existing authorization. ' +
        ' Did you mean to invoke newSpotifyAuthToken instead?')
  }

  if (Date.now() < userData['spotifyAccessTokenExpires']) {
    // The token has not expired, there is no need to refresh yet.
    return;
  }

  // We are authorizing this class using a previously stored refresh token.
  reqParams.append('grant_type', 'refresh_token');
  // Pass in the refresh token that we were given in the past.
  reqParams.append('refresh_token', refreshToken);
  // Pass in our client id so spotify can identify our app.
  reqParams.append('client_id', spotify.getCredentials().clientId);
  // Pass in our secret app password.
  reqParams.append('client_secret', spotify.getCredentials().clientSecret);


  let response: AxiosResponse;

  try {
    response = await axios.post(
        spotify.tokenServiceUrl,
        reqParams
    );
  } catch (error) {
    // This means the user has revoked the refresh token.
    if (error.response.data['error'] === 'invalid_grant') {
      console.error('Failed to refresh authorization for user: ' + uid);
      console.error(error);
      await Promise.all([
        purgeListeningHistoryOfUser(uid, 'short_term'),
        purgeListeningHistoryOfUser(uid, 'medium_term'),
        purgeListeningHistoryOfUser(uid, 'long_term'),
      ]);
      await admin.firestore().collection('users').doc('uid').delete();
    }

    throw new Error();
  }



  // Update the user's record in our database.
  // Store the new tokens.
  const userRecordUpdate: any = {
    spotifyAccessToken: response.data['access_token'],
    // Store the expiration date so we know when we need to renew our token again.
    spotifyAccessTokenExpires: response.data['expires_in'] * 1000 + Date.now(),
  };
  let essentialUserDataBackupCompleted: Promise<any> = Promise.resolve();
  if (response.data['refresh_token']) {
    // Replace the old refresh token with the new one we were just issued.
    userRecordUpdate.spotifyRefreshToken = response.data['refresh_token'];
    essentialUserDataBackupCompleted = admin.firestore().collection('essentialUserData')
        .doc(uid)
        .set({
          spotifyRefreshToken: userRecordUpdate.spotifyRefreshToken
        });
  } else {
    console.warn('Spotify did not provide a refresh token for user: ' + uid);
  }
  // Submit database change.
  const userRecordUpdateCompleted = admin.firestore().collection('users').doc(uid)
      .set(userRecordUpdate, { merge: true });

  await Promise.all([userRecordUpdateCompleted, essentialUserDataBackupCompleted]);

  console.log('Successfully renewed Spotify authorization for user: ' + uid);
};

/**
 * Transforms a Spotify temporary auth code into a long-term access token and potentially a refresh token.
 * This function does not expect the existence of a user record in the database.
 * If there is a pre-existing record, it is updated, otherwise, a new record is created.
 */
export const newSpotifyAuthToken = async (spotifyAuthCode: string, firebaseAuthUID: string) => {
  // Make sure that the user didn't bypass our front-end email validation.
  // Only FSU students are allowed to participate.
  // If they made it this far in the auth process without an FSU email, then they are acting maliciously.
  // Do not proceed.
  const firebaseAuthUser = await admin.auth().getUser(firebaseAuthUID);
  if (!(
      firebaseAuthUser.email!.endsWith('@my.fsu.edu') ||
      firebaseAuthUser.email!.endsWith('@fsu.edu') ||
      firebaseAuthUser.email!.endsWith('@magnet.fsu.edu')
  )) {
    return;
  }

  // This will contain all the parameters that Spotify expects us to provide
  // when requesting a fresh auth token.
  const reqParams = new URLSearchParams();

  // We are authorizing this class using a previously stored refresh token.
  reqParams.append('grant_type', 'authorization_code');
  // Pass in the refresh token that we were given in the past.
  reqParams.append('code', spotifyAuthCode);
  // Pass in our client id so spotify can identify our app.
  reqParams.append('client_id', spotify.getCredentials().clientId);
  // Pass in our secret app password.
  reqParams.append('client_secret', spotify.getCredentials().clientSecret);
  // This must match the redirect_uri that the user client sent Spotify during
  // the initial call to Spotify's authorization service.
  // This function itself does no redirecting however.
  reqParams.append('redirect_uri', getEnvironment().spotifyCredentialsReceiverUrl);

  const response = await axios.post(
      spotify.tokenServiceUrl,
      reqParams
  );

  if (response.data['error']) {
    console.error('Failed to convert authorization code into longer term access token. Firebase User UID: : ' + firebaseAuthUID);
    console.error(response.data['error']);
    throw new Error();
  }

  // Update the user's record in our database.
  // Store the new tokens.
  const userRecordUpdate: any = {
    spotifyAccessToken: response.data['access_token'],
    // Store the expiration date so we know when we need to renew our token again.
    spotifyAccessTokenExpires: response.data['expires_in'] * 1000 + Date.now(),
    // The user's account was created after August 11 2020.
    // We have access to their playback status.
    grantedNowPlayingAccess: true
  };
  // If we were issued a refresh token, store it as well.
  // Spotify does not guarantee a new refresh token however.
  // In this case, we will no longer have access to the user's data after
  // our current access token expires. At that point the user will be purged from
  // the system.
  let essentialUserDataBackupComplete: Promise<any> = Promise.resolve();
  if (response.data['refresh_token']) {
    // Replace the old refresh token with the new one we were just issued.
    userRecordUpdate.spotifyRefreshToken = response.data['refresh_token'];

    // A user's Spotify refresh token is the single most important piece of information in the entire database.
    // Every other entry in the database can be rebuilt given the user's refresh token.
    // It is imperative that we don't ever loose a user's refresh token.
    // Store the refresh token in the user's record AND in a separate less volatile document that serves as an
    // emergency backup.
    essentialUserDataBackupComplete = admin.firestore().collection('essentialUserData')
        .doc(firebaseAuthUID)
        .set({
          spotifyRefreshToken: userRecordUpdate.spotifyRefreshToken,
        });
  }
  // Submit database change.
  const userRecordUpdateCompleted = admin.firestore().collection('users').doc(firebaseAuthUID)
      .set(userRecordUpdate, { merge: true });

  await Promise.all([userRecordUpdateCompleted, essentialUserDataBackupComplete]);
};

const getTopTracksQuery = (spotifyTimeRange: string) => {
  return admin.firestore().collection(spotifyTimeRange + '-tracks')
      .orderBy('count', 'desc')
      .orderBy('firstAppeared', 'desc')
      .orderBy('random', 'desc')
      .limit(50);
}

export const getFSUTopTracks = functions.https.onCall(async (data, context) => {
  const { spotifyTimeRange } = data;

  if (!supportedSpotifyTimeRanges.includes(spotifyTimeRange)) {
    throw new Error('The client requested an unknown or unsupported time range: ' + spotifyTimeRange);
  }

  const { docs } = (await getTopTracksQuery(spotifyTimeRange).get());

  return docs.map(doc => doc.data());
});

export const getFSUTopArtists = functions.https.onCall(async (data, context) => {
  const { spotifyTimeRange } = data;

  if (!supportedSpotifyTimeRanges.includes(spotifyTimeRange)) {
    throw new Error('The client requested an unknown or unsupported time range: ' + spotifyTimeRange);
  }

  const { docs } = (await admin.firestore().collection(spotifyTimeRange + '-artists')
      .orderBy('count', 'desc')
      .limit(50)
      .get());

  return docs.map(doc => doc.data());
});

export const userDidSignup = functions.auth.user().onCreate(async () => {
  const data = {
    signUpCounter: FieldValue.increment(1)
  }
  await admin.firestore().collection('misc').doc('kvStore').set(data, { merge: true });
});

export const userWasDeleted = functions.auth.user().onDelete(async () => {
  const data = {
    signUpCounter: FieldValue.increment(-1)
  }
  await admin.firestore().collection('misc').doc('kvStore').set(data, { merge: true });
});

export const getSignUpCount = functions.https.onCall(async (data, context) => {
  const kvStoreDocRef = admin.firestore().collection('misc').doc('kvStore');
  const kvStoreDoc = await kvStoreDocRef.get();

  const response = {
    count: 0
  };

  if (kvStoreDoc.exists) {
    const docContents = kvStoreDoc.data()!;
    if (docContents['signUpCounter']) {
      response.count = docContents['signUpCounter']
    }
  }

  return response;
});

export const syncUser = functions.pubsub._topicWithOptions('syncUser', {
  // Don't overload the Spotify API
  maxInstances: 1
}).onPublish((message, context) => {
  const { firebaseAuthUID, spotifyTimeRange } = message.json;
  console.log('Syncing user' + firebaseAuthUID);

  // https://stackoverflow.com/questions/57149416/firebase-cloud-function-exits-with-code-16-what-is-error-code-16-and-where-can-i
  // Avoid floating promise by returning it to Firebase executor.
  // Firebase will report a code 16 error if we do not notify it of ongoing promises.
  return admin.firestore().collection('users').doc(firebaseAuthUID).get()
      .then(() => renewSpotifyAuthToken(firebaseAuthUID))
      .then(() => collectListeningHistory(firebaseAuthUID, spotifyTimeRange))
      .then(() => {
        console.log('Successfully collected listening history from user: ' + firebaseAuthUID);
      })
      .catch((error) => {
        console.error('Failed to sync user ' + firebaseAuthUID);
        console.error(error);
        // There's no need to propagate this error up the syncAllUsers.
        // The issue has already been reported in the log file.
      });
});

export const updateSpotifyPlaylist = functions
    // Update the FSU Top 50 and Popular Artists data on a daily basis.
    .pubsub.topic('updateSpotifyPlaylist')
    .onPublish(async (message, context) => {
      const duncan = await admin.auth().getUserByEmail('dbp19a@my.fsu.edu');

      const { spotifyTimeRange } = message.json;
      const playlistId: string = getEnvironment().spotify.playlists[spotifyTimeRange];
      const playlistApiUrl = `https://api.spotify.com/v1/playlists/${playlistId}`;

      await renewSpotifyAuthToken(duncan.uid);

      const userDoc = await admin.firestore().collection('users').doc(duncan.uid).get();

      // Pass Spotify authorization keys to Axios http client.
      const httpClientConfig = {
        headers: {
          // Get the Spotify access code for the user.
          'Authorization': 'Bearer ' + userDoc.data()!['spotifyAccessToken']
        }
      };

      try {
        // Get all the tracks currently stored in the playlist.
        let currentPlaylistState = (await axios.get(playlistApiUrl + '?fields=snapshot_id,tracks(total)&market=ES', httpClientConfig)).data;

        let positions = [];
        for (let i = 0; i < currentPlaylistState.tracks.total; i++) {
          positions.push(i);
        }

        if (positions.length > 0) {
          // Delete all the tracks.
          await axios.delete(playlistApiUrl + '/tracks', {
            ...httpClientConfig,
            data: {
              snapshot_id: currentPlaylistState.snapshot_id,
              positions
            }
          });
        }

        console.log('Done clearing playlist.');

        // Add the new tracks.
        const { docs } = (await getTopTracksQuery(spotifyTimeRange).get());
        const uris = docs
            .map(doc => doc.data())
            .filter((data) => data['count'] > 1)
            .map((data) => data.spotifyUri);

        await axios({
          method: 'POST',
          url: playlistApiUrl + '/tracks',
          ...httpClientConfig,
          data: { uris }
        });

      } catch (error) {
        console.error(error);
      }

      console.log(`Successfully updated Spotify Top 50 Playlist (time range: ${spotifyTimeRange})`);
    });

export const updateAllPlatformPlaylists = functions.pubsub.schedule('every 24 hours').onRun(async () => {
  const pubsub = new PubSub({
    projectId: process.env.GCLOUD_PROJECT,
  });
  const topic = pubsub.topic('updateSpotifyPlaylist');

  await Promise.all(
      supportedSpotifyTimeRanges
          .map(spotifyTimeRange => ({ spotifyTimeRange }))
          .map(payload => topic.publishMessage({ json: payload }))
  );
});

const addUserSyncTasksToQueue = async (firebaseAuthUID: string, spotifyTimeRanges: string[] = supportedSpotifyTimeRanges) => {
  const pubsub = new PubSub({
    projectId: process.env.GCLOUD_PROJECT,
  });

  await Promise.all(
      spotifyTimeRanges
          .map(spotifyTimeRange => {
                return pubsub.topic('syncUser').publishMessage({
                  json: {
                    spotifyTimeRange,
                    firebaseAuthUID
                  }
                });
              }
          )
  );
}

export const frequentRefreshTimerTask = functions.runWith({
      // The maximum timeout allowed by Firebase.
      timeoutSeconds: 540, // 9 minutes
      // The maximum memory capacity allowed by Firebase.
      memory: '2GB'
    })
    // Update the FSU Top 50 and Popular Artists data on a daily basis.
    .pubsub.schedule('every 12 hours')
    .onRun(async (context) => {
      const userIds = (await admin.firestore().collection('users').listDocuments())
          .map(user => user.id);

      const synchronizers = userIds.map(id => addUserSyncTasksToQueue(id, ['short_term']))

      await Promise.all(synchronizers);
});

export const lessFrequentRefreshTimerTask = functions.runWith({
  // The maximum timeout allowed by Firebase.
  timeoutSeconds: 540, // 9 minutes
  // The maximum memory capacity allowed by Firebase.
  memory: '2GB'
})
    // Update the FSU Top 50 and Popular Artists data on a daily basis.
    .pubsub.schedule('every 48 hours')
    .onRun(async (context) => {
      const userIds = (await admin.firestore().collection('users').listDocuments())
          .map(user => user.id);

      const synchronizers = userIds.map(id => addUserSyncTasksToQueue(id, ['medium_term','long_term']))

      await Promise.all(synchronizers);
    });

// noinspection JSUnusedGlobalSymbols
export const spotifyTemporaryCredentialsReceiver = functions.https.onRequest((request, response) => {
  // The temporary auth code that Spotify issued us. This can be used to obtain a long-term authorization token.
  const spotifyAuthCode = request.query['code'] as string;
  // The UID of the the Firebase user that this authorization pertains to.
  // This data was passed to Spotify by our client earlier in the auth procedure.
  const firebaseAuthUID = request.query['state'] as string;
  // This field is populated by Spotify if the user did not grant us with permission to access
  // their account.
  const errorOccurred = Boolean(request.query['error']);
  // If Spotify reports an error during the authorization process, stop now and
  // show an error message on the client.
  if (errorOccurred) {
    console.error('Spotify raised an error immediately. Did the user deny permission?');
    console.error(request.query['error']);
    response.send({
      success: false
    });
    return;
  }
  // Attempt to obtain an access token using the temporary auth code.
  return newSpotifyAuthToken(spotifyAuthCode, firebaseAuthUID)
      .then(() => addUserSyncTasksToQueue(firebaseAuthUID))
      // Show the user a success message and thank them for linking their account.
      .then(() => response.redirect(getEnvironment().frontendUrl + '?showContributionSuccessMessage=true'))
      .catch((error) => {
        console.error(error);
        response.send({ success: false })
      });
});



/**
 * Poll Spotify for now playing data of specific user.
 * Store now playing data in Cloud Firestore.
 * Invoked by triggerNowPlayingDataFetch
 */
export const collectNowPlayingDataOfUser = functions.https.onRequest((request, response) => {
  const { firebaseAuthUID } = request.body;

  // Make sure our Spotify token is valid and hasn't expired.
  return renewSpotifyAuthToken(firebaseAuthUID)
      // Get the user data from Cloud firestore
      .then(() => admin.firestore().collection('users').doc(firebaseAuthUID).get())
      .then(userDoc => {
        // Older versions of HearTheSpear did not request access to the user's currently playing track.
        // Make sure that this user has granted us permission to view their now playing songs.
        // This will always be true for users created after August 11 2020.
        const isAllowedByUser = userDoc.data()!['grantedNowPlayingAccess'];

        if (!isAllowedByUser) {
          return null;
        }

        console.log('Polling playback state of user: ' + firebaseAuthUID);
        return axios.get(
            spotify.nowPlayingSongUrl,
            {
              headers: {
                'Authorization': 'Bearer ' + userDoc.data()!['spotifyAccessToken']
              }
            })
      })
      .then(spotifyResponse => {
        return admin.firestore().runTransaction(async transaction => {
          const userDoc = admin.firestore().collection('users').doc(firebaseAuthUID);
          const userData = (await transaction.get(userDoc)).data()!;
          // The Spotify ID of the song the user was last listening to.
          // https://developer.spotify.com/documentation/web-api/#spotify-uris-and-ids
          const oldSongId: string = userData['lastListenedToSongId'];

          // There might not be an old song ID yet. For example: A request for now playing music
          // has not been made since this user joined Hear The Spear.
          if (oldSongId) {
            // Firestore document that represents a song that was played by an FSU student recently.
            const nowPlayingSongDoc = admin.firestore().collection('nowPlayingSongs').doc(oldSongId);
            // This data is old and stale. Remove it from the database. We will add new data in momentarily.
            transaction.set(nowPlayingSongDoc, {
              count: FieldValue.increment(-1)
            }, { merge: true });
            // Now that we've decremented this now playing counter for this song.
            // Remove the song from the user's profile.
            transaction.set(userDoc, {
              lastListenedToSongId: FieldValue.delete()
            }, { merge: true });
          }

          // If this is null, we do not have permission to check the user's currently playing track.
          // If status is 204, the user is not currently playing a track.
          // Spotify's documentation warns to check for a null "item" field.
          // HearTheSpear is a music tracking service, not a podcast tracker.
          if (spotifyResponse !== null && spotifyResponse.status !== 204 && spotifyResponse.data['item'] !== null
            && spotifyResponse.data['currently_playing_type'] === 'track') {
            const nowPlayingSongDoc = admin.firestore().collection('nowPlayingSongs')
                .doc(spotifyResponse.data['item']['id']);

            // The user is currently listening to this song, increment the count by 1.
            transaction.set(nowPlayingSongDoc, {
              count: FieldValue.increment(1),
              ...convertSpotifyTrackToHearTheSpearTrack(spotifyResponse.data['item'])
            }, { merge: true });

            console.log(`User ${firebaseAuthUID} is currently listening to ${spotifyResponse.data['item']['name']}`)

            transaction.set(userDoc, {
              lastListenedToSongId: spotifyResponse.data['item']['id']
            }, { merge: true });
          } else {
            console.log(`User ${firebaseAuthUID} is not listening to anything right now.`);
          }
        })
      })
      .then(() => response.status(200).end())
      .catch(error => {
        console.error('An error occurred while updating Now Playing data for user: ' + firebaseAuthUID);
        console.error(error);
        // There's no need to propagate this error up to the triggering function.
        // We already logged about it here.
        response.status(200).end();
      });
});

/**
 * Invoked by the web client when fresh now playing data is requested by the user, this happens automatically
 * ever fifteen seconds. If a data fetch is already in progress, the function will terminate immediately.
 */
export const triggerNowPlayingDataFetch = functions.runWith({
    // The maximum timeout allowed by Firebase.
    timeoutSeconds: 540, // 9 minutes
    // The maximum memory capacity allowed by Firebase.
    memory: '2GB'
  })
  .https.onCall(async () => {
      // Protect ourselves from a malicious actor hitting this endpoint a bunch and using up our Spotify rate limit.
      const kvStoreDocRef = admin.firestore().collection('misc').doc('kvStore');
      const hasEnoughTimeElapsed = await admin.firestore().runTransaction(async transaction => {
        const kvStoreDoc = await transaction.get(kvStoreDocRef);
        // Won't exist the first time we run the app or after a reset.
        if (kvStoreDoc.exists) {
          const lastTime = kvStoreDoc.data()!['lastNowPlayingDataFetch'];

          if (lastTime) {
            const cooldownPeriod = 30 * 1000;
            const soonestSafeInvocation = lastTime + cooldownPeriod;
            // If at least 30 seconds has not passed, then do not fetch data from the Spotify API.
            if (Date.now() < soonestSafeInvocation) {
              return false;
            }
          }
        }
        // Record this invocation.
        transaction.set(kvStoreDocRef, {
          lastNowPlayingDataFetch: Date.now()
        }, { merge: true });

        // Enough time has passed, we can proceed.
        return true;
      });
      if (hasEnoughTimeElapsed) {
        console.log('Enough time has elapsed. Continuing with data fetch procedure.')
      } else {
        console.log('Not enough time has elapsed since the last data fetch.');
        return;
      }

      // If this firebase document exists, a data fetcher is already running. There is no need to start another one.
      const isAlreadyRunningDocRef = admin.firestore().collection('flags')
          .doc('fetchingNowPlayingData');
      // Whether or not another fetcher is already running. This is true when there is another client browsing the page
      // right now.
      const isAlreadyRunning = await admin.firestore().runTransaction(async (transaction) => {
        // The firestore document that represents this global flag.
        const isAlreadyRunningDoc = await transaction.get(isAlreadyRunningDocRef);
        if (isAlreadyRunningDoc.exists) {
          // There is data fetcher already running.
          return true;
        }
        // A fetcher is not running, therefore we will start one now.
        // Set the "already running" flag in the database, so other clients wont start duplicates.
        // That would be unnecessary and we'd hit Spotify's rate limit really fast.
        await transaction.create(isAlreadyRunningDocRef, {
          info: 'The existence of this document means a Now Playing Data Fetcher is currently running.',
          created: new Date().toISOString()
        });
        return false;
      });

      if (isAlreadyRunning) {
        // There is a data fetcher already running. No need to start another one.
        console.info('There is a data fetcher already running. Halting now.');
        return;
      }

      // A collection of all the users who have given us permission to access their currently playing track.
      const userIds = (await admin.firestore().collection('users')
          .listDocuments())
          .map(ref => ref.id);

      // The number of data collectors to run at the same time. Limiting this number ensures we don't overwhelm
      // the Spotify API and incur the wrath of their rate limiting.
      const concurrencyLimit = 5;
      const numberOfUsersToQuery = userIds.length;
      // The number of batches we will need to run to hit all our users.
      const batches = Math.ceil(numberOfUsersToQuery / concurrencyLimit);

      for (let batchNo = 0; batchNo < batches; batchNo++) {
        const dataFetchers = [];
        for (let i = 0; i < concurrencyLimit; i++) {
          if (userIds.length < 1) {
            break;
          }

          const firebaseAuthUID = userIds.pop();
          dataFetchers.push(
              axios.post(getEnvironment().collectNowPlayingDataUrl, { firebaseAuthUID })
                  .catch((error) => {
                    console.error('An error occurred while collecting the now playing data of user: ' + firebaseAuthUID);
                    console.error(error);
                  })
          );
        }
        await Promise.all(dataFetchers);
      }
      // We're done with all users.
      // Remove the flag and wait for another keep alive request.
      await isAlreadyRunningDocRef.delete();
});
