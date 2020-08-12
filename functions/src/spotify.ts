import { getEnvironment } from "./environment";

export const getCredentials = () => getEnvironment().spotify;

export const tokenServiceUrl = 'https://accounts.spotify.com/api/token';

export const nowPlayingSongUrl = 'https://api.spotify.com/v1/me/player/currently-playing';
