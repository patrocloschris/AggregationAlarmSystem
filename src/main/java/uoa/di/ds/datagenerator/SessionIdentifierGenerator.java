package uoa.di.ds.datagenerator;

import java.security.SecureRandom;
import java.util.Random;
import java.math.BigInteger;

public final class SessionIdentifierGenerator {
  private SecureRandom random = new SecureRandom();

  public String nextSessionId() {
    return new BigInteger(130, random).toString(32);
  }
  
  public String generateString(Random rng, String characters, int length)
  {
      char[] text = new char[length];
      for (int i = 0; i < length; i++)
      {
          text[i] = characters.charAt(rng.nextInt(characters.length()));
      }
      return new String(text);
  }
}