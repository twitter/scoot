package ice

type intBox struct {
	i int
}

func MakeIntBox1() intBox {
	return intBox{i: 1}
}

type Storage interface {
	Set(i int)
	Get() int
}

type memStorage struct {
	val int
}

func (s *memStorage) Set(i int) { s.val = i }
func (s *memStorage) Get() int  { return s.val }

func NewMemStorage() Storage { return &memStorage{} }

type Auther interface {
	Auth(token string) error
}

type DB struct {
	s Storage
	a Auther
}

func NewDB(s Storage, a Auther) DB { return DB{s, a} }

func (db DB) IsEven(token string) (bool, error) {
	if err := db.a.Auth(token); err != nil {
		return false, err
	}
	return db.s.Get()%2 == 0, nil
}

func (db DB) Inc(token string) error {
	if err := db.a.Auth(token); err != nil {
		return err
	}
	db.s.Set(db.s.Get() + 1)
	return nil
}

type yesAuther struct{}

func (a *yesAuther) Auth(token string) error { return nil }

func NewYesAuther() Auther { return &yesAuther{} }

type Evener struct {
	odder *Odder
}

func (e *Evener) IsEven(i int) bool {
	if i == 0 {
		return true
	}
	return e.odder.IsOdd(i - 1)
}

func MakeEvener(o *Odder) *Evener {
	return &Evener{o}
}

type Odder struct {
	evener *Evener
}

func (o *Odder) IsOdd(i int) bool {
	return o.evener.IsEven(i - 1)
}

func MakeOdder(e *Evener) *Odder {
	return &Odder{e}
}
