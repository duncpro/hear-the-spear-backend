# HearTheSpear
This is the backend system for [HearTheSpear.com](https://HearTheSpear.com), the Florida State Music Chart.

## Features
- Enrollment/Employment at FSU is verified by @fsu.edu email.
- Securely integrated with Spotify using OAuth flow.
- Tracks the most popular songs at FSU over the past few weeks.
- Tracks the most popular artists at FSU other the past few weeks.
- Realtime list of tracks playing at FSU.
- Trending songs and tracks are re-aggregated daily.


## Implementation Overview
- Built on Google's Firebase platform using a Node.js serverless/lambda based architecture.
- Song rank based on how often it appears in every users top 50 most played songs. 
In other words, songs that are shared between many users / appear in many users top 50 are ranked higher.
- Listening history gathered concurrently per-user on a daily basis from Spotify's HTTP API. (Secured with OAuth)
- Realtime "Now Playing" activity lazily refreshed using client heartbeat mechanism. System doesn't spam Spotify when no one is 
onsite, compute time stays in free tier.
- Listening habits stored in Cloud Firestore Realtime Database.
- Album Art and Songs Previews provided by Spotify's Content API.
- Verification emails sent to FSU inboxes using Firebase Authentication.
- For information on the Angular frontend see [here](https://github.com/duncpro/hear-the-spear-frontend).

