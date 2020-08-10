import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';
import * as spotify from './spotify';
import axios from 'axios';
import FieldValue = admin.firestore.FieldValue;
import { getEnvironment } from "./environment";

admin.initializeApp();

const purgeListeningHistoryOfUser = async (firebaseAuthUID: string) => {
  // Get the user record from the database.
  const userDoc = await admin.firestore().collection('users').doc(firebaseAuthUID).get();

  const batch = admin.firestore().batch();

  if (userDoc.data()!['tracks']) {
    for (const trackId of userDoc.data()!['tracks']) {
      // Get a reference to the counter document for this track.
      const trackDoc = admin.firestore().collection('tracks').doc(trackId);
      // Decrement the count by one, since this user's history is being purged.
      batch.update(trackDoc, {
        count: FieldValue.increment(-1)
      });
    }
  }

  if (userDoc.data()!['artists']) {
    for (const artistId of userDoc.data()!['artists']) {
      // Get a reference to the counter document for this artist.
      const artistDoc = admin.firestore().collection('artists').doc(artistId);
      // Decrement the count by one, since this user's history is being purged.
      batch.update(artistDoc, {
        count: FieldValue.increment(-1)
      });
    }
  }

  // Remove the tracks and artists from this user's individual record.
  const update = {
    artists: FieldValue.delete(),
    tracks: FieldValue.delete()
  };
  batch.set(userDoc.ref, update, { merge: true });

  // Submit the database changes to Firebase.
  await batch.commit();
};

/**
 * Collect the listening history of a Spotify user and stores it in the database.
 * This function assumes that there is a Spotify access token stored for the user and that
 * the token is fresh. If you are unsure whether or not a token is refresh, check the expiration date,
 * and if necessary, invoke {@link renewSpotifyAuthToken}.
 * Purges old listening history automatically.
 * @param firebaseAuthUID self explanatory
 */
const collectListeningHistory = async (firebaseAuthUID: string) => {
  /**
   * Returns the Spotify endpoint that holds recent listening history.
   * @param dataType acceptable data types are tracks, and artists.
   * @param limit the number of records to return, the maximum value is fifty.
   */
  const getSpotifyEndpoint = (dataType: string, limit: number = 50) =>
      `https://api.spotify.com/v1/me/top/${dataType}?limit=${limit}&time_range=short_term`;

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

  // Request the user's listening history from the Spotify Web API.
  const [userFavoriteTracksResponse, userFavoriteArtistsResponse] = await Promise.all([
      axios.get(getSpotifyEndpoint('tracks'), httpClientConfig),
      axios.get(getSpotifyEndpoint('artists'), httpClientConfig),
      // Simultaneously purge the user's old listening habits from our records.
      purgeListeningHistoryOfUser(firebaseAuthUID)
  ]);

  const userFavoriteTracks = userFavoriteTracksResponse.data['items'];
  const userFavoriteArtists = userFavoriteArtistsResponse.data['items'];

  // Create a new database transaction.
  // This will contain all the changes we are about to make.
  const batch = admin.firestore().batch();

  for (const track of userFavoriteTracks) {
    // Example Spotify Track URI: spotify:track:12345
    const trackId = track['uri'].split(':')[2];
    // A reference to the track counter document.
    // This contains a count of all the current users who frequently listen to this song.
    const doc = admin.firestore().collection('tracks').doc(trackId);
    const update = {
      // Increment the count by 1, as we have just discovered another frequent listener.
      count: FieldValue.increment(1),
      // Store the name of the track, in case this is the first time it has been recorded in our system.
      name: track['name'],
      // Store the artist of the track, in case this is the first time it has been recorded in our system.
      artist: track['album']['artists'][0]['name'],
      // Store the album of the track, in case this is the first it has been recorded in our system.
      album: track['album']['name'],
      // Store the artwork, in case this is the first it has been recorded in our system.
      // The last image in the list is always the smallest, perfect for our icons.
      art: track['album']['images'].pop()['url'],
      // Store the preview url, in case this is the first the track has been recorded in our system.
      preview: track['preview_url'],
      // Store a link to the long, in case this is the first the song has been recorded in our system.
      link: track['external_urls']['spotify']
    };
    batch.set(doc, update, { merge: true });
  }

  for (const artist of userFavoriteArtists) {
    // Example Spotify Artist URI: spotify:artist:12345
    const artistId = artist['uri'].split(':')[2];
    // A reference to the counter document.
    // This contains a count of all the current users who frequently listen to this artist.
    const doc = admin.firestore().collection('artists').doc(artistId);
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
  const userRecordUpdate = {
    // @ts-ignore
    tracks: userFavoriteTracks.map(track => track['uri'].replace('spotify:track:', '')),
    // @ts-ignore
    artists: userFavoriteArtists.map(artist => artist['uri'].replace('spotify:artist:', ''))
  };
  batch.set(userDoc.ref, userRecordUpdate, { merge: true });

  // Submit the database changes to Firebase.
  await batch.commit();
};


/**
 * Spotify 3rd party application auth tokens periodically expire.
 * Use the refresh auth token to request a fresh new auth token.
 * Store the new token in the database.
 * @param uid The Firebase Auth UID that represents the user.
 */
const renewSpotifyAuthToken = async (uid: string) => {
  functions.logger.info('Attempting to renew Spotify authorization for user: ' + uid);

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
  reqParams.append('client_id', spotify.credentials.clientId);
  // Pass in our secret app password.
  reqParams.append('client_secret', spotify.credentials.clientSecret);

  const response = await axios.post(
      spotify.tokenServiceUrl,
      reqParams
  );

  if (response.data['error']) {
    functions.logger.error('Failed to refresh authorization for user: ' + uid);
    functions.logger.error(response.data['error']);
    throw new Error();
  }

  // Update the user's record in our database.
  // Store the new tokens.
  const userRecordUpdate = {
    spotifyAccessToken: response.data['access_token'],
    // Store the expiration date so we know when we need to renew our token again.
    spotifyAccessTokenExpires: response.data['expires_in'] * 1000 + Date.now(),
    // The old refresh token has been used up, delete it.
    refreshToken: FieldValue.delete()
  };
  // If we were issued a refresh token, store it as well.
  // Spotify does not guarantee a new refresh token however.
  // In this case, we will no longer have access to the user's data after
  // our current access token expires. At that point the user will be purged from
  // the system.
  if (response.data['refresh_token']) {
    // Replace the old refresh token with the new one we were just issued.
    userRecordUpdate.refreshToken = response.data['refresh_token'];
  }
  // Submit database change.
  await admin.firestore().collection('users').doc(uid)
      .set(userRecordUpdate, { merge: true });

  functions.logger.info('Successfully renewed Spotify authorization for user: ' + uid);
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
  if (!(firebaseAuthUser.email!.endsWith('@my.fsu.edu') || firebaseAuthUser.email!.endsWith('@fsu.edu'))) {
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
  reqParams.append('client_id', spotify.credentials.clientId);
  // Pass in our secret app password.
  reqParams.append('client_secret', spotify.credentials.clientSecret);
  // This must match the redirect_uri that the user client sent Spotify during
  // the initial call to Spotify's authorization service.
  // This function itself does no redirecting however.
  reqParams.append('redirect_uri', getEnvironment().spotifyCredentialsReceiverUrl);

  const response = await axios.post(
      spotify.tokenServiceUrl,
      reqParams
  );

  if (response.data['error']) {
    functions.logger.error('Failed to convert authorization code into longer term access token. Firebase User UID: : ' + firebaseAuthUID);
    functions.logger.error(response.data['error']);
    throw new Error();
  }

  // Update the user's record in our database.
  // Store the new tokens.
  const userRecordUpdate = {
    spotifyAccessToken: response.data['access_token'],
    // Store the expiration date so we know when we need to renew our token again.
    spotifyAccessTokenExpires: response.data['expires_in'] * 1000 + Date.now(),
    // If the user is already authenticated, an old refresh token might be stored in the system.
    // We can delete it as it will most likely be replaced with a new one in a second.
    refreshToken: FieldValue.delete()
  };
  // If we were issued a refresh token, store it as well.
  // Spotify does not guarantee a new refresh token however.
  // In this case, we will no longer have access to the user's data after
  // our current access token expires. At that point the user will be purged from
  // the system.
  if (response.data['refresh_token']) {
    // Replace the old refresh token with the new one we were just issued.
    userRecordUpdate.refreshToken = response.data['refresh_token'];
  }
  // Submit database change.
  await admin.firestore().collection('users').doc(firebaseAuthUID)
      .set(userRecordUpdate, { merge: true });
};

export const getFSUTopTracks = functions.https.onCall(async (data, context) => {
  const { docs } = (await admin.firestore().collection('tracks')
      .orderBy('count', 'desc')
      .limit(50)
      .get());

  return docs.map(doc => doc.data()).filter((doc) => doc['count'] > 1);
});

export const getFSUTopArtists = functions.https.onCall(async (data, context) => {
  const { docs } = (await admin.firestore().collection('artists')
      .orderBy('count', 'desc')
      .limit(50)
      .get());

  return docs.map(doc => doc.data()).filter((doc) => doc['count'] > 1);;
});

/**
 * Collects listening history from a given user, refreshing their Spotify authorization if necessary.
 */
export const syncExistingUser = functions.https.onRequest((request, response) => {
  const { firebaseAuthUID } = request.body;

  admin.firestore().collection('users').doc(firebaseAuthUID).get()
      .then(() => renewSpotifyAuthToken(firebaseAuthUID))
      .then(() => collectListeningHistory(firebaseAuthUID))
      .then(() => {
        functions.logger.info('Successfully collected listening history from user: ' + firebaseAuthUID);
        response.status(200);
        // For some reason are functions are timing out if I don't do this.
        process.exit(0);
      })
      .catch((error) => {
        functions.logger.error('Failed to sync user ' + firebaseAuthUID);
        console.error(error);
        response.status(200);
      });
});

/**
 * Syncs all users asynchronously.
 */
export const syncAllUsers = functions
    .runWith({
      timeoutSeconds: 540,
      memory: '2GB'
    })
    .pubsub.schedule('every day')
    .onRun(async (context) => {
  const users = await admin.firestore().collection('users').listDocuments();

  users
      .map(user => user.id)
      .forEach(firebaseAuthUID => axios.post(getEnvironment().syncEndpoint, { firebaseAuthUID }))
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
    functions.logger.error('Spotify raised an error immediately. Did the user deny permission?');
    functions.logger.error(request.query['error']);
    response.send({
      success: false
    });
    return;
  }
  // Attempt to obtain an access toke using the temporary auth code.
  newSpotifyAuthToken(spotifyAuthCode, firebaseAuthUID)
      // Collect the listening history of the user immediately.
      // That way they will see their music reflected in the rankings right off the bat.
      // This will be especially useful while the user base is small.
      .then(() => collectListeningHistory(firebaseAuthUID))
      // Show the user a success message and thank them for linking their account.
      .then(() => response.redirect(getEnvironment().frontendUrl + '?showContributionSuccessMessage=true'))
      .catch((error) => {
        functions.logger.error(error);
        response.send({ success: false })
      });
});
