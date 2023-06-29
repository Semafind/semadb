package kvstore

/* This file contains the string suffix and prefix constants used by the KVStore.
 * We are keeping that as short as possible to save space. */

// ---------------------------

const DELIMITER = "/"

// ---------------------------

const WAL_PREFIX = "W" + DELIMITER
const USER_PREFIX = "U" + DELIMITER
const COLLECTION_PREFIX = "C" + DELIMITER

// ---------------------------
var INTERNAL_SUFFIX = []byte(DELIMITER + "I")
