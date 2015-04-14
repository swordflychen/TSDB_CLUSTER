#ifndef CRC_H
#define CRC_H

/**
 * Calculates the CRC32 of a given data
 * @param begin The input data
 * @param count The length of the data
 * @return The calculated CRC32
 */
unsigned calculateCrc(unsigned char *begin, unsigned count);

#endif
