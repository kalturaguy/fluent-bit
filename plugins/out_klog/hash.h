/***************************************************************************
 *cr
 *cr            (C) Copyright 1995-2019 The Board of Trustees of the
 *cr                        University of Illinois
 *cr                         All Rights Reserved
 *cr
 ***************************************************************************/

/***************************************************************************
 * RCS INFORMATION:
 *
 *      $RCSfile: hash.c,v $
 *      $Author: johns $        $Locker:  $             $State: Exp $
 *      $Revision: 1.17 $      $Date: 2019/01/17 21:21:03 $
 *
 ***************************************************************************
 * DESCRIPTION:
 *   A simple hash table implementation for strings, contributed by John Stone,
 *   derived from his ray tracer code.
 ***************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fluent-bit/flb_mem.h>


typedef struct hash_node_t {
  void* data;                           /* data in hash node */
  const char * key;                   /* key for hash lookup */
  struct hash_node_t *next;           /* next node in hash chain */
} hash_node_t;

typedef struct hash_t {
   struct hash_node_t **bucket;      /* array of hash nodes */
   int size;                         /* size of the array */
   int entries;                      /* number of entries in table */
   int downshift;                    /* shift count, used in hash function */
   int mask;                         /* used to select bits for hashing */
} hash_t;


/*
 *  hash() - Hash function returns a hash number for a given key.
 *
 *  tptr: Pointer to a hash table
 *  key: The key to create a hash number for
 */
static int hash(const hash_t *tptr, const char *key) {
  int i=0;
  int hashvalue;
 
  while (*key != '\0')
    i=(i<<3)+(*key++ - '0');
 
  hashvalue = (((i*1103515249)>>tptr->downshift) & tptr->mask);
  if (hashvalue < 0) {
    hashvalue = 0;
  }    

  return hashvalue;
}

/*
 *  hash_init() - Initialize a new hash table.
 *
 *  tptr: Pointer to the hash table to initialize
 *  buckets: The number of initial buckets to create
 */
static void hash_init(hash_t *tptr, int buckets) {

  /* make sure we allocate something */
  if (buckets==0)
    buckets=16;

  /* initialize the table */
  tptr->entries=0;
  tptr->size=2;
  tptr->mask=1;
  tptr->downshift=29;

  /* ensure buckets is a power of 2 */
  while (tptr->size<buckets) {
    tptr->size<<=1;
    tptr->mask=(tptr->mask<<1)+1;
    tptr->downshift--;
  } /* while */

  /* allocate memory for table */
  tptr->bucket=(hash_node_t **) flb_calloc(tptr->size, sizeof(hash_node_t *));

  return;
}

/*
 *  hash_lookup() - Lookup an entry in the hash table and return a pointer to
 *    it or HASH_FAIL if it wasn't found.
 *
 *  tptr: Pointer to the hash table
 *  key: The key to lookup
 */
static void* hash_lookup(const hash_t *tptr, const char *key) {
  int h;
  hash_node_t *node;


  /* find the entry in the hash table */
  h=hash(tptr, key);
  for (node=tptr->bucket[h]; node!=NULL; node=node->next) {
    if (!strcmp(node->key, key))
      break;
  }

  /* return the entry if it exists, or HASH_FAIL */
  return(node ? node->data : NULL);
}

/*
 *  hash_insert() - Insert an entry into the hash table.  If the entry already
 *  exists return a pointer to it, otherwise return HASH_FAIL.
 *
 *  tptr: A pointer to the hash table
 *  key: The key to insert into the hash table
 *  data: A pointer to the data to insert into the hash table
 */
static int hash_insert(hash_t *tptr, const char *key, size_t key_len,void* data) {
  int tmp;
  hash_node_t *node;
  int h;

  /* insert the new entry */
  h=hash(tptr, key);
  node=(struct hash_node_t *) flb_malloc(sizeof(hash_node_t));
  node->data=data;
  node->key=flb_strndup(key,key_len);
  node->next=tptr->bucket[h];
  tptr->bucket[h]=node;
  tptr->entries++;

  return (int)node;
}

/*
 *  hash_delete() - Remove an entry from a hash table and return a pointer
 *  to its data or HASH_FAIL if it wasn't found.
 *
 *  tptr: A pointer to the hash table
 *  key: The key to remove from the hash table
 */
static void* hash_delete(hash_t *tptr, const char *key) {
  hash_node_t *node, *last;
  void* data;
  int h;

  /* find the node to remove */
  h=hash(tptr, key);
  for (node=tptr->bucket[h]; node; node=node->next) {
    if (!strcmp(node->key, key))
      break;
  }

  /* Didn't find anything, return HASH_FAIL */
  if (node==NULL)
    return NULL;

  /* if node is at head of bucket, we have it easy */
  if (node==tptr->bucket[h])
    tptr->bucket[h]=node->next;
  else {
    /* find the node before the node we want to remove */
    for (last=tptr->bucket[h]; last && last->next; last=last->next) {
      if (last->next==node)
        break;
    }
    last->next=node->next;
  }

  /* free memory and return the data */
  data=node->data;
  flb_free(node->key);
  flb_free(node);

  return(data);
}


/*
 * inthash_entries() - return the number of hash table entries.
 *
 */
static int hash_entries(hash_t *tptr) {
  return tptr->entries;
}




/*
 * hash_destroy() - Delete the entire table, and all remaining entries.
 * 
 */
static void hash_destroy(hash_t *tptr, void (*cb) (const char *,          /* key  */
                                      void * /* value */) ) {
  hash_node_t *node, *last;
  int i;

  for (i=0; i<tptr->size; i++) {
    node = tptr->bucket[i];
    while (node != NULL) { 
      last = node;   
      node = node->next;
      if (cb!=NULL) {
          cb(last->key,last->data);
      }
      flb_free(last->key);
      flb_free(last);
    }
  }     

  /* free the entire array of buckets */
  if (tptr->bucket != NULL) {
    flb_free(tptr->bucket);
    memset(tptr, 0, sizeof(hash_t));
  }
}

