package kvstore

/* This file contains the string suffix and prefix constants used by the KVStore.
 * We are keeping that as short as possible to save space. */

// ---------------------------

const DELIMITER = "/"

const GRAVEYARD_PREFIX = "G" + DELIMITER

var TOMBSTONE = []byte{0x7f} // Del key in ascii

// ---------------------------

const REPLOG_PREFIX = "R" + DELIMITER
const USER_PREFIX = "U" + DELIMITER
const COLLECTION_PREFIX = "C" + DELIMITER
const SHARD_PREFIX = "S" + DELIMITER
const POINT_PREFIX = "P" + DELIMITER

// ---------------------------
